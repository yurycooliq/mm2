"""One immutable research report, two views; no execution or publication effects."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import re
from pathlib import Path
from zoneinfo import ZoneInfo
from collections import Counter

from .archive import read_member, verify_bundle
from .schema import ResearchError, canonical, integer, load_json, utc


def metric(value, unit, lane, quality, reason, source, population, count=None):
    return {"value": value, "unit": unit, "lane": lane, "quality": quality, "reason": reason,
            "source_refs": [source], "population": population, "sample_count": count}


def snapshot_utc(value):
    # Python collectors use +00:00; archive contracts use Z. Both are explicit UTC.
    if isinstance(value, str) and value.endswith("+00:00"):
        value = value[:-6] + "Z"
    return utc(value)


def build_report(bundle, *, cutoff_utc, display_timezone="Europe/Lisbon", controller=None, engine=None, diagnostic_snapshot=None, access=None):
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
    identity = {"bundle_id": source, "cutoff_utc": cutoff_utc, "display_timezone": display_timezone,
                "controller": controller, "engine": engine, "diagnostic_snapshot": diagnostic_snapshot, "access": access}
    report_id = "mm2-control-" + hashlib.sha256(canonical(identity)).hexdigest()
    sources = {source: {"kind": "verified_research_packet", "bundle_id": source,
                       "manifest_sha256": hashlib.sha256((Path(bundle) / "manifest.json").read_bytes()).hexdigest(),
                       "application_sha": manifest.get("source_revision"),
                       "source_at_utc": manifest["created_at_utc"]}}
    report = {"schema_version": 2, "report_id": report_id,
              "cutoff_utc": cutoff_utc, "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "period": manifest["utc_range"], "period_closed": utc(manifest["utc_range"]["end"]) <= cutoff,
              "display_timezone": display_timezone, "display_cutoff": cutoff.astimezone(zone).isoformat(),
              "packet_age_seconds": int((cutoff - created).total_seconds()),
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
        observed = snapshot_utc(controller["captured_at_utc"])
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
    if engine is not None:
        observed = snapshot_utc(engine["captured_at_utc"])
        if observed > cutoff:
            raise ResearchError("engine snapshot postdates cutoff")
        from scripts.research_export import safe_health
        health = safe_health(engine.get("health", {}))
        safe = {key: health.get(key) for key in ("execution_mode", "mode", "api_ready", "feed_ready", "live_eligible", "fault_class", "published_at_ms")}
        report["engine_snapshot"] = {"quality": "PARTIAL", "current_health": None,
                                     "reason": "OBSERVED_SNAPSHOT_NOT_CONTINUOUS_UPTIME",
                                     "observed_at_utc": engine["captured_at_utc"],
                                     "age_seconds": int((cutoff - observed).total_seconds()), "observed_state": safe}
        report["host_eligibility"] = {key: engine.get("geoblock", {}).get(key) for key in ("blocked", "country", "region")}
        report["sources"]["engine"] = {"kind": "allowlisted_authenticated_cached_engine_snapshot",
                                       "sha256": hashlib.sha256(canonical(engine)).hexdigest()}
    if diagnostic_snapshot is not None:
        observed = snapshot_utc(diagnostic_snapshot["captured_at_utc"])
        if observed > cutoff:
            raise ResearchError("diagnostic snapshot postdates cutoff")
        from scripts.research_export import safe_diagnostics
        safe = safe_diagnostics(diagnostic_snapshot)
        counts = Counter((row["source"], row["message"]) for row in safe["incidents"])
        report["recent_diagnostics"] = {
            "observed_at_utc": diagnostic_snapshot["captured_at_utc"],
            "age_seconds": int((cutoff - observed).total_seconds()),
            "hours": safe["hours"], "systemd": safe["systemd"], "containers": safe["containers"],
            "engine_source_revision": safe.get("engine_source_revision"),
            "observed_rows": len(safe["incidents"]), "crash_episode_count": None,
            "quality": "PARTIAL", "historic_root_cause_proven": False,
            "groups": [{"source": source, "message": message, "rows": count}
                       for (source, message), count in sorted(counts.items())]}
        report["sources"]["recent_diagnostics"] = {"kind": "bounded_allowlisted_incident_capture",
                                                   "sha256": hashlib.sha256(canonical(diagnostic_snapshot)).hexdigest()}
    if access is not None:
        observed = snapshot_utc(access["captured_at_utc"])
        if observed > cutoff:
            raise ResearchError("access snapshot postdates cutoff")
        statuses = {"AUTHENTICATED", "EXPIRED", "UNCONFIGURED", "DENIED", "UNREACHABLE",
                    "CLI_UNAVAILABLE", "AUTH_ERROR", "INVALID_IDENTITY_RESPONSE"}
        if (access.get("status") not in statuses
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", str(access.get("profile", "")))
                or not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]", str(access.get("region", "")))):
            raise ResearchError("invalid access snapshot")
        report["research_access"] = {key: access[key] for key in ("status", "profile", "region")}
        report["research_access"].update({"observed_at_utc": access["captured_at_utc"],
                                           "age_seconds": int((cutoff - observed).total_seconds()),
                                           "scope": "AUTHENTICATION_ONLY_NOT_ENGINE_HEALTH"})
        report["sources"]["research_access"] = {"kind": "read_only_access_preflight",
                                                   "sha256": hashlib.sha256(canonical(access)).hexdigest()}
    report["next_actions"] = [
        {"priority": 1, "action": "Получить полный журнал MM2: начало/конец периода, движения средств, исполнения и связь с заявками.",
         "done_when": "Сверены деньги/позиции и полнота; каждое исполнение связано с MM2."},
        {"priority": 2, "action": "Проверить контекст диагностических окон и неполные границы расходов.",
         "done_when": "Нет подстановки нулей и переноса новых counters на старые события."},
        {"priority": 3, "action": "Сравнить зарегистрированный кандидат с baseline на новых данных.",
         "done_when": "Учтены расходы, задержки, конечный капитал и худшие потери; отдельный экономический verdict."}]
    operational_actions = []
    if "research_access" in report and report["research_access"]["status"] != "AUTHENTICATED":
        operational_actions.append({"action": "Восстановить исследовательский AWS-доступ и получить новые срезы сервера и controller.",
                                    "done_when": "STS и read-only проверки успешны; отдельно проверены состояние engine и время источников."})
    recent = report.get("recent_diagnostics", {})
    if any("fatal=true" in row["message"] for row in recent.get("groups", [])):
        operational_actions.append({"action": "Установить причину fatal feed-сбоев: различить переполнение очереди и закрытие потребителя; сопоставить с lifecycle и нагрузкой.",
                                    "done_when": "Проверена причина, исправление прошло нагрузочную проверку и длительную приемку без потери событий."})
    if report.get("host_eligibility", {}).get("blocked") is True:
        operational_actions.append({"action": "Проверить разрешенность серверного подключения у площадки и выбрать поддерживаемое размещение.",
                                    "done_when": "Получено подтверждение допуска; серверная проверка blocked=false. Локальный IP не управляет engine."})
    report["next_actions"] = operational_actions + report["next_actions"]
    for priority, action in enumerate(report["next_actions"], 1):
        action["priority"] = priority
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


REASON_TEXT = {
    "ACTUAL_TRADE_FEES_UNKNOWN": "Не подтверждены фактические комиссии сделок",
    "CASH_UNKNOWN": "Не подтвержден остаток денежных средств",
    "CLOSING_SNAPSHOT_NOT_AT_WINDOW_BOUNDARY": "Конечный срез не совпадает с границей периода",
    "CLOSING_STATE_UNVERIFIED": "Не подтверждено состояние счета в конце периода",
    "COMPLETE_SET_TOKEN_MAPPING_REQUIRED": "Нужно сопоставить парные токены рынка",
    "CONDITIONAL_POSITION_VALUATION": "Оценка открытых позиций зависит от неподтвержденных условий",
    "EVENT_ACCOUNTING_AUTHORITY_UNPROVEN": "Нет первичного подтверждения финансовых событий",
    "OPENING_STATE_UNKNOWN": "Не известно состояние счета в начале периода",
    "PROFILE_ATTRIBUTION_REVERIFY_REQUIRED": "Нужно повторно подтвердить связь профиля и счета",
    "PUBLIC_ACTIVITY_NOT_CANONICAL_RECEIPT": "Публичная активность не заменяет подтверждения исполнений",
    "PUBLIC_ACTIVITY_RECONCILIATION_REQUIRED": "Публичные события еще не сверены с финансовым журналом",
    "PUBLIC_CASH_UNKNOWN": "Публичный источник не подтверждает денежный остаток",
    "PUBLIC_OWNER_ACTIVITY_MM2_ATTRIBUTION_UNPROVEN": "Не доказано, что действия счета совершил MM2",
    "SNAPSHOT_MISSING": "Отсутствует необходимый срез счета",
    "SOURCE_FINANCIAL_COMPLETENESS_UNPROVEN": "Полнота финансовых данных не подтверждена",
    "STARTING_CAPITAL_UNKNOWN": "Не подтвержден начальный капитал",
    "MM2_ORDER_INTENT_ATTRIBUTION_REQUIRED": "Исполнения нужно связать с заявками собственного бота",
    "REDEMPTION_FINALITY_UNVERIFIED": "Не подтверждено окончательное погашение позиций",
    "REDEMPTION_TOKEN_MAPPING_REQUIRED": "Погашение нужно сопоставить с токенами",
    "REPORTED_PRICE_NOT_NATIVE_MICRO_ALIGNED": "Цена источника не соответствует точности площадки",
}


def render_full(report):
    rows = ["# MM2: контрольный отчет", "",
            f"Период UTC: {report['period']['start']} — {report['period']['end']} (правая граница исключена).",
            f"Срез: {report['display_cutoff']}. Источник: `{report['report_id']}`.", "",
            f"Возраст исследовательского пакета на срез: {report['packet_age_seconds']} s. Метрики относятся к указанному периоду, а не ко всему дню.", "",
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
                 "Причины неполноты:"]
        rows += ["- " + REASON_TEXT.get(reason, "Не пройден контроль данных: " + reason) + "." for reason in item["reasons"]]
        if not item["reasons"]:
            rows.append("- Не подтверждена связь событий с собственным ботом.")
    diag = report["diagnostics"]
    snapshot = report["engine_snapshot"]
    engine_text = "Состояние engine неизвестно: нужен timestamp; суточная доступность требует полного heartbeat interval."
    if "observed_state" in snapshot:
        state = snapshot["observed_state"]
        engine_text = f"Срез engine {snapshot['observed_at_utc']}: {state['execution_mode']} / {state['mode']}, API ready={state['api_ready']}, feeds ready={state['feed_ready']}, live eligible={state['live_eligible']}. Возраст {snapshot['age_seconds']} s; это не суточный uptime."
    rows += ["", "## Состояние и сбои", "", engine_text,
             f"Диагностических строк: {diag['observed_incident_rows']}; окон backfill: {diag['backfill_windows']}.",
             "Количество отдельных падений и историческая причина неизвестны; строки не равны падениям."]
    if "research_service" in report:
        service = report["research_service"]
        rows.append(f"Research service: {service.get('status')}; наблюдение {service['observed_at_utc']}, возраст {service['age_seconds']} s. Это срез, не uptime.")
    if "research_access" in report:
        access = report["research_access"]
        rows.append(f"Исследовательский AWS-доступ ({access['profile']}, {access['region']}): {access['status']}; проверка {access['observed_at_utc']}, возраст {access['age_seconds']} s. Ошибка доступа не доказывает остановку engine.")
    if "host_eligibility" in report:
        venue = report["host_eligibility"]
        rows.append(f"Ответ площадки для сервера: blocked={venue['blocked']}, country={venue['country']}, region={venue['region']}; это не юридическое заключение о владельце счета.")
    if "recent_diagnostics" in report:
        recent = report["recent_diagnostics"]
        rows += [f"Последняя доступная диагностика: {recent['observed_at_utc']}, возраст {recent['age_seconds']} s, окно {recent['hours']} h, строк {recent['observed_rows']}.",
                 f"Наблюдаемый systemd NRestarts={recent['systemd'].get('NRestarts', 'неизвестно')}; это не число падений за отчетный период.",
                 "", "| Источник | Диагностический код | Строк |", "| --- | --- | --- |"]
        for group in recent["groups"]:
            rows.append(f"| {group['source']} | {group['message']} | {group['rows']} |")
    rows += ["", "## Следующие действия", ""]
    for action in report["next_actions"]:
        rows.append(f"{action['priority']}. {action['action']} Готово, когда: {action['done_when']}")
    return "\n".join(rows) + "\n"


def render_telegram(report):
    result = display(report["metrics"]["live_bot_net_pnl"])
    state = report["engine_snapshot"].get("observed_state")
    operation = (f"Engine: {state['execution_mode']} / {state['mode']} (срез {report['engine_snapshot']['observed_at_utc']}, возраст {report['engine_snapshot']['age_seconds']} s).\n" if state else "Текущее состояние engine неизвестно.\n")
    if "research_access" in report:
        access = report["research_access"]
        operation += f"AWS-доступ: {access['status']} (проверка {access['observed_at_utc']}, возраст {access['age_seconds']} s). Ошибка доступа не доказывает остановку engine.\n"
    if report.get("host_eligibility", {}).get("blocked") is True:
        operation += "На указанном срезе сервера площадка сообщила blocked=true.\n"
    recent = report.get("recent_diagnostics", {})
    if any("fatal=true" in row["message"] for row in recent.get("groups", [])):
        operation += f"Диагностика {recent['observed_at_utc']} (возраст {recent['age_seconds']} s): наблюдались fatal feed-сбои; причина требует проверки.\n"
    return (f"MM2 · срез {report['display_cutoff']}\n"
            f"Период UTC: {report['period']['start']} — {report['period']['end']}\n"
            f"Возраст исследовательского пакета: {report['packet_age_seconds']} s.\n"
            f"{operation}"
            f"Чистый результат собственного бота: {result}.\n"
            "Нет полного журнала исполнений и атрибуции MM2. Публичная активность не доказывает прибыль.\n"
            f"Диагностических строк: {report['diagnostics']['observed_incident_rows']}; число падений неизвестно.\n"
            f"Первый приоритет: {report['next_actions'][0]['action']}\n"
            f"Источник: {report['report_id']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--out", required=True, help="new output directory; never overwrites an earlier report")
    parser.add_argument("--cutoff-utc", required=True)
    parser.add_argument("--timezone", default="Europe/Lisbon")
    parser.add_argument("--controller-snapshot")
    parser.add_argument("--engine-snapshot")
    parser.add_argument("--diagnostic-snapshot")
    parser.add_argument("--access-snapshot")
    args = parser.parse_args()
    report = build_report(args.bundle, cutoff_utc=args.cutoff_utc, display_timezone=args.timezone,
                          controller=load_json(args.controller_snapshot) if args.controller_snapshot else None,
                          engine=load_json(args.engine_snapshot) if args.engine_snapshot else None,
                          diagnostic_snapshot=load_json(args.diagnostic_snapshot) if args.diagnostic_snapshot else None,
                          access=load_json(args.access_snapshot) if args.access_snapshot else None)
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
