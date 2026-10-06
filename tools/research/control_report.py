"""One immutable research report, two views; no execution or publication effects."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
from zoneinfo import ZoneInfo

from .archive import read_member, verify_bundle
from .schema import ResearchError, canonical, integer, load_json, utc


def metric(value, unit, lane, quality, reason, source, population, count=None):
    return {"value": value, "unit": unit, "lane": lane, "quality": quality, "reason": reason,
            "source_refs": [source], "population": population, "sample_count": count}


def build_report(bundle, *, cutoff_utc, display_timezone="Europe/Lisbon", controller=None):
    manifest = verify_bundle(bundle)
    if manifest["scope"].get("kind") != "research-packet" or manifest["scope"].get("lane") != "public-benchmark":
        raise ResearchError("control report requires a verified public research packet")
    cutoff = utc(cutoff_utc)
    created = utc(manifest["created_at_utc"])
    if created > cutoff:
        raise ResearchError("packet was not available at report cutoff")
    zone = ZoneInfo(display_timezone)
    accounts = read_member(bundle, "accounting-summary.json")
    diagnostics = read_member(bundle, "diagnostics.json")
    provenance = read_member(bundle, "provenance.json")
    if not isinstance(accounts, dict) or set(accounts) != {"mm2-public", "mo-money"}:
        raise ResearchError("expected exactly the two registered public-account summaries")
    source = manifest["bundle_id"]
    sources = {source: {"kind": "verified_research_packet", "bundle_id": source,
                       "manifest_sha256": hashlib.sha256((Path(bundle) / "manifest.json").read_bytes()).hexdigest(),
                       "application_sha": manifest.get("source_revision"),
                       "source_at_utc": manifest["created_at_utc"]}}
    report = {"schema_version": 2, "report_id": "mm2-control-" + source,
              "cutoff_utc": cutoff_utc, "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "period": manifest["utc_range"], "period_closed": utc(manifest["utc_range"]["end"]) <= cutoff,
              "display_timezone": display_timezone, "display_cutoff": cutoff.astimezone(zone).isoformat(),
              "scope": "public research; no authenticated bot-only live ledger",
              "sources": sources, "metrics": {}, "account_observations": {},
              "profitability_verdict": "INCONCLUSIVE", "primary_mutations": False}
    metrics = report["metrics"]
    for name, unit in (("live_bot_net_pnl", "collateral_pico"), ("live_bot_unique_fills", "fills"),
                       ("live_bot_roi", "ratio"), ("live_bot_drawdown", "collateral_pico")):
        metrics[name] = metric(None, unit, "live", "UNKNOWN", "FULL_AUTHENTICATED_BOT_LEDGER_AND_ATTRIBUTION_REQUIRED",
                               source, "MM2-only authenticated account in report period")
    for account, accounting in accounts.items():
        if accounting.get("lane") != "public-benchmark":
            raise ResearchError("accounting lane differs from public research scope")
        q = accounting.get("quality", {})
        complete = q.get("status") == "COMPLETE" and q.get("eligibility", {}).get("accounting") is True
        net = integer(accounting.get("net_pnl_pico"), signed=True, nullable=True)
        components = accounting.get("components", {})
        coverage = components.get("cost_evidence_coverage", {})
        # Legacy packets have no bound coverage; never trust their None-to-zero total.
        bound_complete = (coverage.get("source_window_complete") is True
                          and type(coverage.get("missing_cost_bound")) is int and coverage["missing_cost_bound"] == 0
                          and type(coverage.get("cost_bound_records")) is int
                          and coverage["cost_bound_records"] == accounting.get("event_count"))
        bound = integer(components.get("conservative_cost_bound_pico"), nullable=True) if bound_complete else None
        report["account_observations"][account] = {
            "period": accounting.get("utc_range"), "accounting_inputs_hash": accounting.get("inputs_hash"),
            "quality": q.get("status", "UNKNOWN"), "reasons": q.get("reasons", []),
            "ownership_attribution": "UNVERIFIED; public wallet may contain manual or other-bot activity",
            "wallet_net_pnl": metric(str(net) if complete and net is not None else None, "collateral_pico",
                                     "public-benchmark", "COMPLETE" if complete and net is not None else "UNKNOWN",
                                     "PUBLIC_ACCOUNT_ONLY_NOT_MM2_PROFIT" if complete and net is not None else "INCOMPLETE_ACCOUNTING",
                                     source, "observed public-wallet account, not bot-attributed"),
            "declared_event_cost_bound": metric(str(bound) if bound is not None else None, "collateral_pico",
                                                "public-benchmark", "COMPLETE" if bound is not None else "UNKNOWN",
                                                "DECLARED_EVENT_COSTS_ONLY" if bound is not None else "COST_BOUND_COVERAGE_UNPROVEN",
                                                source, "declared cost bounds for reconciled events"),
            "observed_cost_bound_subtotal": metric(components.get("observed_cost_bound_pico"), "collateral_pico",
                                                   "public-benchmark", "PARTIAL" if components.get("observed_cost_bound_pico") is not None else "UNKNOWN",
                                                   "KNOWN_DECLARATIONS_ONLY_NEVER_COMPLETE_COSTS", source, "observed bound declarations"),
            "observed_activity_events": metric(accounting.get("event_count"), "events", "public-benchmark", "PARTIAL",
                                               "OBSERVED_ACTIVITY_NOT_AUTHENTICATED_UNIQUE_BOT_FILLS", source, "reconciled public activity")}
    windows = diagnostics.get("windows", []) if isinstance(diagnostics, dict) else []
    incidents = diagnostics.get("incidents", []) if isinstance(diagnostics, dict) else []
    report["diagnostics"] = {"observed_incident_rows": len(incidents), "backfill_windows": len(windows),
                             "crash_episode_count": None, "historic_root_cause_proven": False,
                             "lifecycle_windows": windows,
                             "backlog": provenance.get("diagnostic_backlog"),
                             "reason": "ROWS_AND_RESTART_COUNTERS_DO_NOT_ESTABLISH_CRASH_CAUSALITY"}
    health = read_member(bundle, "health.json")
    report["engine_snapshot"] = {"quality": "UNKNOWN", "current_health": None,
                                 "reason": "SNAPSHOT_CAPTURE_TIME_AND_HEARTBEAT_COVERAGE_REQUIRED",
                                 "retained_mode": health.get("mode") if isinstance(health, dict) else None}
    if controller is not None:
        observed = utc(controller["captured_at_utc"])
        if observed > cutoff:
            raise ResearchError("controller snapshot postdates cutoff")
        health = controller["status"].get("service_health", {})
        # Preserve an observation with its age; never turn it into guaranteed uptime.
        report["research_service"] = {key: health.get(key) for key in
                                       ("unit", "status", "active_state", "sub_state", "result", "exit_status", "last_exited_utc")}
        report["research_service"].update({"observed_at_utc": controller["captured_at_utc"],
                                          "age_seconds": int((cutoff - observed).total_seconds()),
                                          "quality": "OBSERVED_SNAPSHOT_NOT_UPTIME"})
        report["sources"]["controller"] = {"kind": "captured_read_only_controller_status",
                                           "sha256": hashlib.sha256(canonical(controller)).hexdigest()}
    report["next_actions"] = [
        {"priority": 1, "action": "Получить полный журнал MM2: начало/конец периода, движения средств, исполнения и связь с заявками.",
         "done_when": "Сверены деньги/позиции и полнота; каждое исполнение связано с MM2."},
        {"priority": 2, "action": "Проверить контекст диагностических окон и неполные границы расходов.",
         "done_when": "Нет подстановки нулей и переноса новых counters на старые события."},
        {"priority": 3, "action": "Сравнить зарегистрированный кандидат с baseline на новых данных.",
         "done_when": "Учтены расходы, задержки, конечный капитал и худшие потери; отдельный экономический verdict."}]
    all_metrics = list(metrics.values())
    for account in report["account_observations"].values():
        all_metrics += [value for value in account.values() if isinstance(value, dict) and "unit" in value]
    for value in all_metrics:
        value.update({"source_at_utc": manifest["created_at_utc"],
                      "observed_at_utc": manifest["created_at_utc"],
                      "timestamp_scope": "packet compilation; financial boundaries are reported separately",
                      "provenance": "PUBLIC_OBSERVED", "coverage": {"denominator": None, "ratio": None}})
    canonical(report)  # Schema secret/tree bounds apply before writing any view.
    return report


def display(metric_value):
    value = metric_value["value"]
    if value is None:
        return "неизвестно"
    if metric_value["unit"] == "collateral_pico":
        return format(Decimal(value) / Decimal(10**12), "f") + " collateral"
    return str(value)


def render_full(report):
    rows = ["# MM2: контрольный отчет", "",
            f"Период UTC: {report['period']['start']} — {report['period']['end']} (правая граница исключена).",
            f"Срез: {report['display_cutoff']}. Источник: `{report['report_id']}`.", "",
            "**Реальная доходность MM2 не установлена:** источник содержит публичные наблюдения, а не полный журнал собственного бота.", "",
            "| Показатель собственного бота | Значение | Качество |", "| --- | --- | --- |"]
    names = {"live_bot_net_pnl": "Чистый финансовый результат", "live_bot_unique_fills": "Подтвержденные исполнения",
             "live_bot_roi": "Доходность капитала", "live_bot_drawdown": "Максимальная просадка"}
    for name, item in report["metrics"].items():
        rows.append(f"| {names[name]} | {display(item)} | {item['quality']} |")
    rows += ["", "## Публичные наблюдения", "", "Наблюдения кошелька могут включать ручные действия и другие боты."]
    for account, item in report["account_observations"].items():
        rows += ["", f"### {account}", "", f"Качество учета: {item['quality']}.",
                 f"Результат наблюдаемого кошелька: {display(item['wallet_net_pnl'])}; это не прибыль MM2.",
                 f"Граница заявленных затрат: {display(item['declared_event_cost_bound'])}.",
                 f"Известная часть границ затрат: {display(item['observed_cost_bound_subtotal'])}.",
                 f"Наблюденные события: {display(item['observed_activity_events'])}; не число сделок собственного бота.",
                 "Причины неполноты: " + (", ".join(item["reasons"]) or "отсутствие атрибуции собственному боту") + "."]
    diag = report["diagnostics"]
    rows += ["", "## Состояние и сбои", "",
             "Текущее состояние engine и суточная доступность неизвестны: нужен timestamp и полный heartbeat interval.",
             f"Диагностических строк: {diag['observed_incident_rows']}; окон backfill: {diag['backfill_windows']}.",
             "Количество отдельных падений и историческая причина неизвестны; строки не равны падениям."]
    if "research_service" in report:
        service = report["research_service"]
        rows.append(f"Research service: {service.get('status')}; наблюдение {service['observed_at_utc']}, возраст {service['age_seconds']} s. Это срез, не uptime.")
    rows += ["", "## Следующие действия", ""]
    for action in report["next_actions"]:
        rows.append(f"{action['priority']}. {action['action']} Готово, когда: {action['done_when']}")
    return "\n".join(rows) + "\n"


def render_telegram(report):
    result = display(report["metrics"]["live_bot_net_pnl"])
    return (f"MM2 · срез {report['display_cutoff']}\n"
            f"Период UTC: {report['period']['start']} — {report['period']['end']}\n"
            f"Чистый результат собственного бота: {result}.\n"
            "Нет полного журнала исполнений и атрибуции MM2. Публичная активность не доказывает прибыль.\n"
            f"Диагностических строк: {report['diagnostics']['observed_incident_rows']}; число падений неизвестно.\n"
            "Дальше: полный учет средств и позиций, затем проверка экономического кандидата.\n"
            f"Источник: {report['report_id']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--out", required=True, help="new output directory; never overwrites an earlier report")
    parser.add_argument("--cutoff-utc", required=True)
    parser.add_argument("--timezone", default="Europe/Lisbon")
    parser.add_argument("--controller-snapshot")
    args = parser.parse_args()
    report = build_report(args.bundle, cutoff_utc=args.cutoff_utc, display_timezone=args.timezone,
                          controller=load_json(args.controller_snapshot) if args.controller_snapshot else None)
    target = Path(args.out)
    target.mkdir(parents=True, exist_ok=False)
    for name, data in (("report.json", canonical(report) + b"\n"),
                       ("report.md", render_full(report).encode("utf-8")),
                       ("telegram.txt", render_telegram(report).encode("utf-8"))):
        with (target / name).open("xb") as stream:
            stream.write(data)
    with (target / "manifest.json").open("xb") as stream:
        stream.write(canonical({"report_id": report["report_id"], "complete": True,
                               "files": [{"path": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                                         for path in sorted(target.iterdir()) if path.is_file() and path.name != "manifest.json"]}) + b"\n")
    print(canonical({"output": str(target), "report_id": report["report_id"],
                     "profitability_verdict": report["profitability_verdict"], "messages_sent": False}).decode())


if __name__ == "__main__":
    main()
