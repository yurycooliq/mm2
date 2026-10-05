# mm2: текущий инженерный срез для ИИ, составляющей суточные отчёты

Дата: **2026-10-05**, часовой пояс владельца: **Europe/Lisbon**.
Это передача контекста текущей локальной рабочей копии. Результаты сборки,
контрактных тестов, симуляции, доступности внешней инфраструктуры и реальной
торговли нужно учитывать раздельно. Предыдущий
[срез 2026-10-01](STATUS_HANDOFF_2026-10-01.md) сохранён как исторический;
его числа тестов, ограничения одного рынка и отсутствие торгового адаптера
не описывают текущий код.

## Запрос и принятые решения

**mm2 / mo-money-2** — самостоятельный Rust-бот для **одного счёта владельца**,
эмулятор биржи и отдельные Telegram-бот/Mini App для управления и вторичной
статистики. Главный инженерный приоритет — корректное быстрое исполнение с
конечными ресурсами и сохранением неизвестных обязательств. Задержка до биржи
измеряется отдельно от CPU стратегии и отзывчивости панели.

AWS отложен из-за подтверждения оплаты. Владелец попросил завершить пакет
**что разворачивать**, затем решать **где**. Новый запрос требует при запуске
выбрать симуляцию, торговлю или обе одновременно. Сервер/регион/co-location,
eligibility конкретного хоста, HTTPS, реальные торговые ключи и Telegram
остаются внешними шагами. Выбор режима не является реальным исполнением ордера.

Репозиторий: [приватный mm2](https://github.com/yurycooliq/mm2).
Локальный checkout: `C:\Users\yuryc\Code\Bot\mm2`, со своим Git отдельно
от внешнего Bot. Срез относится к коммиту, содержащему этот отчёт; его SHA
проверяется через `git log -1 -- docs/STATUS_HANDOFF_2026-10-05.md`. Новый remote CI
проверяется отдельно для нужного SHA; Oct1 baseline нельзя выдавать за CI
текущей версии. Секреты, базы/журналы, локальные сборки и данные не должны
входить в инженерный отчёт или репозиторий.

## Реализованные компоненты и происхождение

| Компонент | Текущая граница |
| --- | --- |
| `mm2-core` | Детерминированная BUY-only модель заявок/резервов/частичных исполнений, cancel-before-replace и неизвестных исходов. |
| `mm2-emulator` | Событийная виртуальная биржа с конечной ликвидностью, очередью, post-only, отдельными post/cancel/notification задержками, 429/обрывами и восстановлением. |
| `mm2-strategy` | Детерминированные ограниченные правила котировок/хеджа; сигналы допускаются только при явной доступности и свежести. |
| `mm2-portfolio` | Общий конечный капитал, позиции по идентичности рынка, комиссии/резервы/риски, idempotent receipts merge/расчёта и восстановление. |
| `mm2-live` | Технический адаптер закреплённого SDK V2: подготовка/подпись, post/cancel, authenticated reconciliation, user stream, heartbeat, отдельный account ledger. |
| `mm2-engine` | Проверка потоков, выбор execution lanes, стратегия, multi-market paper, owner API, точные журналы/receipts и ограниченная публикация. |
| `apps/console`, `apps/dashboard` | Один независимый sampler, SQLite/Telegram owner control и React/Vite Mini App, раздельное отображение real/paper и неизвестных/устаревших значений. |
| Исследовательские инструменты | Pendulumflow provenance/импорт, archive quality, TypeScript reference/parity и read-only измерения между хостами. |
| Эксплуатация | Конечная ротация журнала без удаления evidence, offline verify/backup/restore, startup wrappers и ограниченный read-only soak. |

Это отдельная история Git без runtime-зависимости от Olympus. Shadow/API,
ограниченная работа с архивами и measurement-инструменты адаптированы из
Olympus PR 1462, подготовленного source commit
`41fcec1f3a2294cea74f1bc8ece438889c5537c0`. Детерминированные core/emulator
и owner console созданы для mm2. Strategy rules опираются на явно scoped upstream
`packages/updown-lab/src/strategy/rules.ts`; это происхождение гипотезы, а не
доказательство доходности. Полные границы — в [PROVENANCE.md](PROVENANCE.md)
и [STRATEGY.md](STRATEGY.md).

## Режимы и запуск

| Wrapper mode | Конфигурация | Поведение |
| --- | --- | --- |
| `demo` | `config/demo.toml` | Полностью локальные синтетические данные; native mode `emulator`. |
| `simulation` | `config/simulation.toml` | Публичные наблюдения и виртуальное исполнение. |
| `shadow` | `config/public.toml` | Неотправленные предложения без paper-исполнений. |
| `trading` | `config/trading.toml` | Ограниченный one-account V2 путь после прохождения его gates. |
| `both` | `config/both.toml` | Trading и независимая public-paper линия из общих наблюдений. |

Все запускаются **на паузе**. Wrapper default — public simulation; прямой
Compose без выбора профиля сохраняет demo default. Конфигурация выбирается
только в окружении дочернего процесса, существующий `.env` не переписывается.
Non-dry-run trading/both заранее отклоняет пустой/неавторизованный public template
до Docker. Native `--check` проверяет форму, не реальные credentials или биржу.

```powershell
./scripts/bootstrap.ps1
./scripts/start.ps1 -Mode demo -Build
# Для публичной симуляции вместо предыдущей команды:
./scripts/start.ps1 -Mode simulation -Build
```

```sh
python3 scripts/start.py --mode both --dry-run
```

Resume — отдельная owner-команда. В trading нужны свежая сверка счёта, user stream,
heartbeat, валидные market/deployment identities и конечные risk limits.
`live_eligible` сообщает текущую техническую готовность, а не экономический
допуск или результат торговли. Kill сохраняется; Reset не стирает историю
и не пополняет капитал.

## Исполнение, shared account и durable evidence

- Paper и real средства, заявки, исполнения и receipts разделены. В `both`
  companion paper находится в `state_dir/paper`; он получает bounded feed
  и имеет собственную fault/phase/capacity.
- Packaged trading/both используют **общий**
  `live_state_dir = "/data/account-live"` при разных owner directories
  `/data/trading` и `/data/both`. Переключение профиля сохраняет funded-account
  commitments/spend/loss history и один локальный account fence.
- Профиль привязан к mode/feed/account-ledger location. Account identity
  закрепляет signer/funder/signature/network/protocol/deployment, а не текущие
  risk/capacity/env-reference settings. Перенос того же счёта в новый пустой
  каталог для обхода его лимитов недопустим.
- Signed intent сохраняется и синхронизируется **до** post. Таймаут/неизвестный
  cancel или отсутствие заявки в open-list не освобождают резерв. Authenticated
  известный ID и cumulative fills определяют reconciliation.
- Restart не пересылает старый signed post вслепую. Сохраняются неизвестные
  возможные исполнения и account baseline/owned-fill position evidence;
  запуск возвращается на паузу и требует сверки.
- Owner ID/payload/CAS сохраняются при неопределённом ответе. Разные lane journals
  не образуют распределённую атомарную транзакцию; частичный исход не нужно
  превращать в успех интерфейса.
- Основной owner/paper журнал — общая hash chain через numbered archives,
  `commands.jsonl` и synced rotation seals. Старый single-file формат сохраняется
  при миграции; recovery читает bounded records. Torn/missing/corrupt evidence
  отклоняет запуск.
- Default segment — **8 MiB**, retained total — **128 MiB**, до **128 архивов**,
  **100,000 owner commands** и **250,000 execution operations**. Live default
  retained bound — **64 MiB**; portfolio/receipts/market identities также конечны.
  Первая достигнутая квота прекращает новую работу с сохранёнными obligations.
  Ротация не означает бесконечную автономную работу и не удаляет историю.
- Offline backup acquires owner/paper/live locks, проверяет hash/sequence/schema
  и копирует только известные evidence files. Shared account sidecar указывается
  явно. Restore пишет в **новые** mode/account пути, сохраняет оригиналы и требует
  paused Rust replay validation. Контрольные суммы не являются authenticated
  venue truth или доказательством off-host backup.
- Локальный filesystem fence не обеспечивает cross-host HA/active-active.
  Нельзя выдавать Windows export-lock за lock работающего Linux Docker volume.

Paper-портфель переносит конечный капитал через рынки, сохраняет старые outcome
tokens под прежней идентичностью и перед rollover закрывает возможные исполнения.
Полные пары объединяются явно **синтетически**; demo имеет отдельно маркированный
synthetic resolution. Unpaired public-paper позиции остаются unresolved:
публичная цена не является финальным исходом или доказательством redemption.

Публичные trade/book observations — вход в приблизительную модель очереди/объёма.
Они не доказывают место нашего ордера в биржевой очереди, собственные fills,
отсутствие скрытой ликвидности или реальное время доставки cancel. Эмулятор/replay
дают контрактные и sensitivity evidence, **не торговый доход**.

## Текущие проверки и их область

Подтверждённые результаты текущих локальных проверок:

| Проверка | Результат и область |
| --- | --- |
| Python console | **24 passed**, mocked auth/owner/transport, freshness, real/paper summaries и bounded history. |
| Python ops | **23 passed**, verify/backup/restore/fences, independent capacity/loopback soak и startup preflight; Windows и pinned Linux. |
| Python data/latency | **55 passed**, pinned optional dependencies, без skipped; Parquet/local fake HTTP/WSS fixtures. |
| Node reference + strategy parity | **25 passed = 19 + 6**, scoped archive/execution и fixed-point quote contracts. |
| Dashboard Node regressions | **4 passed**, команда/receipt identity и uncertain outcome recovery. |
| Playwright browser fixtures | **PASS**, три execution profiles, раздельные счета, mobile owner flow и retry исходной неизвестной команды. |
| Финальные Rust checks/release | **137 tests passed**, fmt и Clippy `-D warnings` PASS, locked optimized workspace build PASS. Docker/CI ограничивают test threads двумя; runtime deadlines не ослаблены. |
| Финальная сборка/проверка контейнеров | **Оба новых образа собраны, 13 checks passed** за 23 453 мс: пять профилей, offline отказ blank trading/both, owner/receipts, fills, console outage, crash recovery/fence, graceful stop 859 мс. Live orders: 0. |

Числа строк внутри suite не нужно повторно прибавлять к workspace totals.
Browser mock/staging accounts не финансируемый venue и не реальный Telegram.
Всего **268 автоматических тестов** (137 Rust + 102 Python + 29 Node) и 13
контейнерных проверок. Время остановки — один локальный замер, не SLA сервера.
Актуальный объединённый результат и последующие исправления должны попадать
в [VALIDATION.md](VALIDATION.md), а не угадываться из количества файлов.

Наносекундные benchmark-перцентили core/strategy описывают локальный CPU-контракт.
Исторический 100k core benchmark повторял unchanged two-slot evaluation, когда
обе заявки уже Live; он не отправлял и не отменял 100k биржевых ордеров.
Runtime decision timing исключает transport/parsing/queue wait/execution
durability/publication. Loopback soak измеряет owner API responsiveness.
Ни одно из них не является full post/cancel latency до matching engine.

## Что ещё требуется до funded deployment

1. Проверить новый remote CI для intended SHA; локальные Rust/container checks завершены.
2. Проверить реальный account/signature/funder, deployed protocol/contracts,
   authenticated endpoint/fees/ticks/minima, позиции, unknown outcome recovery
   и heartbeat protection на разрешённом host/account.
3. Выбрать eligible host, проверить TLS/WSS/clock/disk, full post/cancel tails,
   paused backup/restore, внешние alerts и bounded длительный soak всех lanes.
4. Приватно задать intended account/capital/market/risk settings и runtime secret
   references. Ключи не копируются в исходники, изображения или отчёты.
5. Установить HTTPS/exact allowed origin и проверить Telegram owner/initData
   на настоящем клиенте; внешние сообщения пока не отправлялись.
6. Для экономического допуска: eligible Pendulumflow development data,
   governing historical TWAP60, point-in-time availability, frozen candidate,
   untouched holdout, fees, adverse queue/delay cases и конечный капитал.

**Не реализовано в funded adapter:** V3 positions, deposit-wallet EIP-1271,
sells, on-chain merge/redemption. Поддерживается ограниченный Polygon CLOB V2
BUY-only token-order путь для проверенных EOA/canonical proxy/canonical safe.
Receipt-учёт merge/redemption в portfolio не отправляет chain transactions.
Бесконечное рециклирование funded-capital без settlement path не обещается.

Нет реальных funded запусков/исполнений, selected-host суточного soak, deployment
AWS, real Telegram embedding/delivery, экономического holdout или измеренного ROI.
Предыдущий локальный public-readiness transport failure остаётся исторической
неудачей конкретного host; он не доказывает eligibility будущего сервера.

## Как использовать в суточном отчёте

Разделяй **implemented**, **verified locally**, **external pending** и
**unsupported**. Не называй pending проверку успешной и не называй mock capability
production-ready. Доход/PnL/ROI сейчас отсутствуют; simulated cash, retained
collateral, loss bounds, arithmetic conservation и число тестов не являются
доходностью. Не повторяй Oct1 отсутствие gateway/multi-market/rotation как
актуальный дефект — эти модули добавлены, но их реальные внешние gates остаются.

Первичные документы: [README](../README.md), [ARCHITECTURE](ARCHITECTURE.md),
[EXECUTION](EXECUTION.md), [STRATEGY](STRATEGY.md), [OPERATIONS](OPERATIONS.md),
[CONSOLE](CONSOLE.md), [BUILD_CONTRACT](BUILD_CONTRACT.md),
[VALIDATION](VALIDATION.md), [ROADMAP](ROADMAP.md) и
[DATA_AND_LATENCY](DATA_AND_LATENCY.md).
