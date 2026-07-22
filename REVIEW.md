# coral — code review (2026-07-09)

> Historical snapshot. The reviewed branch and the fixes described below were
> later merged to `main`. Do not use this file as current release status; see
> `docs/HISTORY.md`, `docs/REVIEW.md`, and `docs/ROADMAP.md` for the consolidated
> 2026-07-22 baseline and the current stabilization gates.

Первое сквозное код-ревью coral по методике wisp (углы → верификация по коду).
Прочитан весь прод-код (~5.1k LOC, 18 пакетов). Гейт на момент ревью:
`go build ./...` и `go vet ./...` зелёные; 167 тест-функций в 24 файлах (без
тестов только `cmd/coral`, `internal/exporter/devnull`).

Связь с `~/work/yaop/platform-review/coral.md`: тот проход зафиксировал
архитектуру и контрактные несоответствия на глаз. Здесь — подтверждение их
**на уровне кода** (с `file:line`) плюс находки, которые видно только из кода
(помечены **NEW**). Приоритеты: P0 — потеря данных / контракт-критично,
P1 — надёжность/безопасность/консистентность, P2 — качество/наблюдаемость.

Ничего не исправлялось — это список. Фиксить, как wisp, тематическими батчами.

Все находки сверены с единым контрактом платформы (`platform-review/
contract.md`, черновик v0 от 2026-07-07) — соответствие по секциям и
догруз того, что видно только из контракта, в разделе **«Сверка с
контрактом»** ниже.

---

## Статус фиксов (ветка `fix/contract-conformance`, 2026-07-10)

Поверх baseline `60018fd` (импорт текущего рабочего дерева в `main`), 7 фикс-
коммитов, каждый верифицирован откатом, гейт зелёный. НЕ смёржено/НЕ запушено.

**Закрыто:**
- ✅ **P0-2** логи → amber (`85e8c74`).
- ✅ **P0-3** единый retry по §4-таблице, пакет `internal/exporter/backoff`
  (`9b22544`). Осталось из §4: `partial_success` (см. ниже) и 413 — 413 сделан
  в P2-8.
- ✅ **P2-12/#8** cros → fathom (`b1b3040`).
- ✅ **P2-8/§3** gzip + JSON + 415 + **413** на OTLP/HTTP приёмниках, пакет
  `internal/otlphttp` (`c887300`). Покрыты traces/metrics/logs; jaeger/zipkin —
  нет (legacy, вне §3-приоритета).
- ✅ **P1-6/§6** resource-scope у трейс-`attributes` — обогащение (collector=coral,
  k8s.*) теперь доезжает до amber (`597e1a4`).
- ✅ **P1-7/§8 + B-4** validate: редакция creds вместо дропа спана, покрытие
  `Resource.Attrs`, `service.name`=unknown_service (`8b3cb60`).
- ✅ **B-1/B-2/§9** `/readyz` + self-obs `coral_*` (+ throughput метрик/логов) +
  дефолт-порт `:8888`→`:4888` (`cab7935`).

**Закрыто в P0-1-сессии (ветка `fix/contract-conformance`, +3 коммита):**
- ✅ **Q-1** generic `pipeline.Pipeline[T Signal]` (`a7634d1`) — три копии ядра
  свёрнуты в один worker-pool; `model/metric/logs.Batch` реализуют `Len()`;
  `metric`/`logs` держат тонкие алиасы `Pipeline=pipeline.Pipeline[Batch]` +
  `NewPipeline`. Лог-пайплайн получил стадию процессоров. Побочно закрыт
  **P2-10** (metric/log теперь считают `batchesIn/dropped` через общий `Enqueue`).
- ✅ **P0-1/§2** единый OTLP-эндпоинт (`30af5d6`) — новый `receiver/otlp.Server`:
  один gRPC (Trace+Metrics+Logs) + один HTTP-mux (/v1/traces|metrics|logs) на
  4317/4318; регистрируются только сервисы активных пайплайнов (иначе
  Unimplemented/404). Сняты пер-сигнальные приёмники (trace grpc/http, metric/otlp,
  logs/otlp) и блоки `metric_pipeline.receivers`/`log_pipeline.receivers` (метрики/
  логи едут общим ingress). gRPC на backpressure → `codes.Unavailable`. Example-
  конфиги на канон-порты (coral 4317/4318, amber 5318, fathom 6318; 4319-4322
  свёрнуты). App: ingress стартует после пайплайнов, стоп — до них.
- ✅ **P1-7 redaction логов/метрик** (`25b5517`) — новый `internal/otlpredact` +
  `metric.RedactProcessor`/`logs.RedactProcessor` (тип `redact` в
  metric_pipeline/log_pipeline); скрабит creds по key/value во всех scope +
  датапоинтах/рекордах, у логов — ещё и string-body. Мирроринг трейс-`validate`.

- ✅ **B-3 `partial_success`** (`5f0b68a`) — accept-time admission в ingress:
  `Sink.{Trace,Metric,Log}Admit` возвращают (admit-батч, rejected, reason);
  отклонённое едет в `Export*ServiceResponse.PartialSuccess`
  (`rejected_spans`/`rejected_data_points`/`rejected_log_records` + message) на
  gRPC и HTTP. Трейс-admit собирается из `validate.max_span_bytes` (оверсайз-спаны
  теперь режутся на приёме с отчётом, а не молча дропаются в пайплайне; validate-
  процессор остаётся для jaeger/defense-in-depth, дабл-каунта нет — отклонённое не
  энкьюится). Oversize-запрос→413 был закрыт в P2-8. Метрик/лог-admit — каркас
  готов, правил отклонения пока нет (партиал всегда пуст).

**Ниты добиты 2026-07-10 (+3 коммита):**
- ✅ `cd5ba5e` **harden legacy trace receivers** — jaeger UDP `handlePacket` с
  `recover()` (защита от паники декодера); jaeger thrift_http `MaxBytesReader`→413
  (было молчаливое усечение LimitReader→400); zipkin `Timestamp==0` → спан
  отклоняется (было время 1970). Тест на zipkin ts=0.
- ✅ `baa495c` **enforce service.name on metric/log resources** (§6) — новый
  `internal/otlpresource.EnsureServiceName` + `metric/logs.ServiceNameProcessor`,
  always-on первым в обоих пайплайнах (`buildMetric/LogPipeline`). Юнит-тесты +
  app-e2e (метрика и лог без service.name → amber получает `unknown_service`).
- ✅ `e6d16b2` **self-obs ingress counters + delivery semantics** — в `/metrics`
  добавлены `coral_otlp_{requests,errors,accepted_*,rejected_*}` (accepted/rejected
  по всем сигналам, партиал-видимость); README-секции Transport + Delivery
  semantics (at-most-once, §1/§4); at-most-once-комментарий в `pipeline.go`.

**РЕВЬЮ coral ПОЛНОСТЬЮ ЗАКРЫТО.** Гейт зелёный (build/vet/gofmt/`-race` весь
suite; каждый коммит собран изолированно). Ветка `fix/contract-conformance` = 14
фикс-коммитов, НЕ смёржена/НЕ запушена.

---

## P0

### P0-1. Шесть OTLP-портов — стандартный OTLP-клиент ломается
Трейс-, метрик- и лог-пайплайны биндят **каждый свои** otlp_grpc/otlp_http
листенеры: `config.go:51-58` (traces), `config.go:189-192` (metric_pipeline),
`config.go:326-329` (log_pipeline); проводка — `app.go:317-335`,
`app.go:117-124`, `app.go:182-189`. Стандартный OTel SDK/коллектор шлёт
traces+metrics+logs в один `:4317`/`:4318`. Против coral:
- метрики на `:4317` → `Unimplemented` (на трейс-порту зарегистрирован только
  `TraceService`, `otlp/grpc.go:53-54`);
- `POST /v1/metrics` на `:4318` → `404` (мультиплексор трейс-HTTP знает только
  `/v1/traces`, `otlp/http.go:47-48`).

Контракт (`wisp design/02-contract` §4) декларирует «wisp → coral :4317», а
фактический метрик-приёмник — на отдельном порту. **Фикс:** один gRPC-сервер +
один HTTP-mux, регистрирующие все три сервиса и роутящие в соответствующие
пайплайны; старые порты — deprecated-алиасы на переходный период. Разблокируется
дедупликацией ядра (см. Q-1).

### P0-2. Логи физически не могут уехать в amber
`buildLogExporter` (`app.go:211-223`) имеет **только** ветку `cros` (и её же
как default); ветки `amber` нет. Мало того — `LogPipelineConfig.validate`
(`config.go:355-359`) **отвергает** любой `type`, кроме `cros`. То есть логи,
принятые coral, доходят только до fathom/cros и **никогда не достигают стора**.
Комментарий-обоснование («until Amber exposes a matching OTLP log ingest route»)
устарел: amber принимает `/v1/logs`. **Фикс:** log-amber-экспортёр симметрично
метрикам (`metric/amber.go`), снять запрет в валидаторе.

### P0-3. Retry крутит любую ошибку как ретраябельную (3 места)
Нет классификации permanent/transient, игнорируется `Retry-After`, нет джиттера:
- traces: `retry/exporter.go:38-60` — цикл `MaxAttempts` по любому `err != nil`;
- metrics amber: `metric/amber.go:71-88`;
- metrics cros: `metric/amber.go:144-161`.

`400` (битый payload) ретраится столько же, сколько `503`; экспонента без
джиттера → синхронные retry-штормы. В связке с amber, отвечающим `200` на
переполнение очереди (кросс-продуктовая находка №1), coral вообще не узнаёт о
потере. **Фикс:** общая таблица «код → retryable?» (2xx=ok, partial_success=не
ретраить, 429/503/RESOURCE_EXHAUSTED=backoff+Retry-After, 4xx=перманентно),
одна реализация на все экспортёры (сейчас их две независимых).

---

## P1

### P1-4. TLS/auth не просто выключен — его негде включить  **(NEW)**
В конфиге нет полей TLS/bearer ни у одного приёмника (`EndpointConfig` =
только `endpoint`, `config.go:60-63`), ни у одного экспортёра (`AmberConfig`,
`S3Config`, `MetricExporterConfig`, `LogExporterConfig` — без auth-полей).
Приёмники: `grpc.NewServer` без креденшелов (`otlp/grpc.go:53`,
`metric/otlp.go:46`, `logs/otlp.go:45`); `http.Server` без TLS. Экспортёры —
голый `http.Client`. coral — незащищённая середина цепочки
wisp(mTLS+bearer)→coral→amber. **Это ровно то место, куда садится W4/reef —
и сейчас там нет конфиг-поверхности под него.** Стоит спроектировать конфиг TLS/
bearer заранее, чтобы reef-интеграция была врезкой, а не редизайном схемы.

### P1-5. Fan-out последовательный и блокируется retry amber  **(NEW)**
`pipeline.processFrom:161-165` гоняет экспортёры **последовательно** в воркере.
amber обёрнут в retry (`app.go:471-475`) и при недоступности держит воркер до
~Σ(backoff)·attempts, прежде чем батч дойдёт до cros/s3. Медленный/лежащий amber
(1) тормозит fan-out в fathom и (2) не даёт этому воркеру разгребать очередь →
backpressure до приёмников. По контракту amber (source of truth) и fathom
(derived) должны иметь независимые судьбы доставки (coral.md P1-7) — здесь нет.
**Фикс:** fan-out в отдельные горутины per-exporter (или отдельные
under-pipeline'ы), изоляция ошибок и таймингов.

### P1-6. Словарь действий `attributes` расходится traces↔metrics, неизвестное молча игнорируется  **(NEW)**
- trace-процессор: `add/delete/rename` (`processor/attributes.go:13-15`),
  `NewAttributes` **ошибается** на неизвестном действии (`:56-57`);
- metric-процессор: `upsert/insert/delete` (`metric/attributes.go:12,43-69`),
  а `applyAction` в default-ветке **возвращает атрибуты без изменений и без
  ошибки** (`metric/attributes.go:67-68`); `buildMetricPipeline` имена действий
  не валидирует (`app.go:126-141`), `config.validate` для метрик проверяет
  только `type=="attributes"` (`config.go:290-297`).

Итог: `action: add` (трейс-стиль) в `metric_pipeline` — **тихий no-op**,
обогащение k8s/cloud молча не проставляется. Плюс: трейс-`attributes` правит
`span.Attrs`, а метрик-`attributes` — `Resource.Attributes`; обогатить
ресурс-атрибуты трейсов (`collector=coral`, `k8s.*`) сейчас нечем. Семантика
обогащения несогласована по сигналам. **Фикс:** единый набор действий +
fail-stop на неизвестном во всех пайплайнах; определиться, где стоят атрибуты
(resource vs span) для трейсов.

### P1-7. `validate` дропает весь спан на creds-матч и не смотрит resource  **(NEW/усиление coral.md P2-9)**
`processor/validate.go:40-44`: один хит creds-паттерна по любому атрибуту →
**дроп всего спана** (потеря данных), а не редакция самого атрибута. `hasCreds`
(`:54`) сканирует только `s.Attrs`, но не `s.Resource.Attrs` → секрет в
ресурс-атрибуте проходит насквозь. И у метрик, и у логов validate/redaction нет
вообще (лог-пайплайн вовсе без процессоров: `logs.Pipeline` не имеет поля
processors, `app.go:179-199` их не добавляет) — а логи самый частый носитель
секретов. **Фикс:** редактировать значение, не дропать спан; покрыть resource;
дать redaction-процессор логам/метрикам.

---

## P2 / наблюдаемость / контракт

### P2-8. OTLP/HTTP-приёмники игнорируют Content-Type и Content-Encoding
`otlp/http.go:89-125`, `metric/otlp.go:109-132`, `logs/otlp.go:108-131`:
Content-Type не проверяется (JSON-тело → `proto.Unmarshal` падает → `400`
вместо `415`/поддержки JSON), gzip не разжимается (Content-Encoding
игнорируется → gzip-тело → `400`). amber уже умеет JSON; стандартные OTLP/HTTP
экспортёры включают gzip. **Фикс:** роутинг по Content-Type (proto/json),
`Content-Encoding: gzip`, `415` на неизвестное.

### P2-9. Self-obs метрики мислейбл и неполны
`app.go:290-292` отдаёт `collector_batches_in/dropped/spans_out` (по конвенции
платформы — `coral_*`) и **только для трейс-пайплайна**; `metricPipeline.
PointsOut()` и `logPipeline.RecordsOut()` на `/metrics` не выводятся вовсе.
Приёмные счётчики (requests/errs/spansAccepted в атомиках) не экспонируются.
Throughput метрик и логов невидим.

### P2-10. Счётчики drop двойные и считаются до постановки в очередь  **(NEW)**
`pipeline.go:70-77`: `emit` инкрементит `batchesIn` **до** select и при
`ctx.Done` инкрементит `batchesDropped` — дропнутый батч попадает в оба.
Метрик/лог-пайплайны drop не считают вообще (нет счётчика).

### P2-11. Tail-sampling режет трейсы на части  **(NEW)**
`sampling/tail.go`: решение принимается через `decisionWait` после последнего
спана; спаны, пришедшие позже, попадают в `decided`-LRU (keep → эмитятся
поштучно, drop → тихо теряются, `tail.go:116-121`). Если запись из `decided`
вытеснена (размер LRU = `maxTraces*2`), поздние спаны образуют **новый** pending
и переоцениваются независимо (возможно противоположное решение). Оба пути дают
частичные трейсы в amber. Это врождённая плата ограниченного tail-sampling —
но должно быть **явно записано в контракт** (coral.md contract item 5), а не
подразумеваться.

### P2-12. Имя `cros` повсюду
`exporter/cros` (traces), `metric.CROSExporter`, `logs/cros.go`, конфиг-`type`
`cros`, порт 8099. Прямой след research-прототипа; в контракте платформы должно
быть одно имя (fathom). Переименовать тип/конфиг/комментарии.

---

## Ниты / defense-in-depth

- **UDP jaeger без `recover()`** (`thrift_udp.go:55-85`): декодирование в
  горутине без перехвата паники. Парсер обороняется bounds-чеками
  (`thrift.go` readString/readBytes/коллекции валидируют длины, пре-аллокаций по
  attacker-длине нет) — паника маловероятна, но один непокрытый путь уронит весь
  процесс. Дешёвая страховка: `recover()` в цикле чтения пакетов.
- **jaeger thrift_http** использует `io.LimitReader` (`thrift_http.go:36`), а не
  `http.MaxBytesReader`: тело ровно на лимите молча усечётся (→ decode error →
  `400`) вместо явного `413`.
- **gRPC-приёмники** на backpressure возвращают клиенту сырой `ctx.Err()`
  (`otlp/grpc.go:112-114`, `metric/otlp.go:103-105`, `logs/otlp.go:102-104`)
  вместо gRPC-статуса `codes.Unavailable`.
- **zipkin**: `EndTime = UnixMicro(Timestamp+Duration)` при `Timestamp==0`
  (в zipkin опционально) даёт время 1970 вместо отказа (`zipkin/convert.go:66-67`).
- **Спула нет** → at-most-once внутри coral. Для single-node приемлемо
  (durability на wisp-spool + amber-WAL). Это уже зафиксировано в контракте
  как осознанное решение (`contract.md §1`) — coral остаётся объявить
  соответствие в README + кодом-комментарием, а не «дописывать в контракт».

---

## Сверка с контрактом (`platform-review/contract.md`)

Проверено против единого контракта v0. Три части: (A) находки выше, которые
контракт **подтверждает** и где надо поднять ранг; (B) находки, которые видит
**только** контракт — я их в первом проходе упустил (догруз, verified по коду
2026-07-09); (C) что coral должен *отдать* в контракт.

### A. Соответствие и переоценка ранга

| Находка REVIEW | Контракт | Правка ранга |
|---|---|---|
| P0-1 (6 портов) | §2 таблица портов + §1 «wisp собирает все 3 сигнала» | Подтверждён. Цель фикса — **строго 4317/4318** (coral = вход платформы, не трогаем), amber уходит на 5317/5318 (снимает коллизию #15). И P0-1 — **блокер** пути «wisp все сигналы» (§1 следствие б), не просто удобство клиента. |
| P0-2 (логи не в amber) | §1 mismatch #5 | Подтверждён. |
| P0-3 (retry без классификации) | **§4 таблица ответов** (готовая: 200/partial_success/400/429-503+Retry-After/413) | Подтверждён. Фиксить прямо по §4-таблице — она уже канонична, «свою» не изобретать. |
| P1-4 (TLS/auth негде) | §8 + reef, порядок миграции wisp→coral→**amber**→fathom | Подтверждён. reef — ратифицированный механизм; W4 = §8 для coral. |
| **P1-6** (attributes: span vs Resource) | **§6**: «Обогащение coral гарантированно добавляет k8s.*/cloud.* и `collector=coral`» | **Поднять до нарушения контракта.** Трейс-`attributes` правит `span.Attrs`, а не `Resource` → coral **не может выполнить §6-гарантию** ресурс-обогащения для трейсов. Это не «несогласованность», это невыполнимый контракт. |
| P1-7 (нет redaction у логов) | §8 «redaction в coral-процессорах (✗ у логов нет процессоров)» | Подтверждён дословно. |
| P2-8 (gzip/content-type) | **§3 РЕШЕНИЕ: gzip обязателен** у всех приёмников + 415 на неизвестный тип | **Не P2, а contract-must.** gzip-приём — ратифицированное требование, не «nice-to-have». |
| P2-12 (имя cros) | §1 mismatch #8 | Подтверждён. |
| Q-2 (cardinality_limit) | **§7: coral = глобальная страховка** (средний ярус defense-in-depth) | **Не «улучшение», а ролевая обязанность.** Отсутствие процессора = coral не закрывает свой ярус контракта. |

### B. Догруз — находки только из контракта (я их упустил) **verified**

- **B-1 (P2/ops). Нет `/readyz`.** §9 требует **и** `/healthz`, **и** `/readyz`
  (единые имена по платформе). coral отдаёт только `/healthz`
  (`otlp/http.go:49`, `app.go:294`); `/readyz` нет нигде. (mismatch #14)
- **B-2 (P2). Self-obs порт неправильный.** §2 схема x888 → coral self-obs =
  **`:4888`**. Во всех example-конфигах `metrics.endpoint: :8888`
  (`configs/collector.example.yaml:64`, `examples/*.yaml`). Плюс сама метрика
  зовётся `collector_*` (уже в P2-9) — по §9 должно быть `coral_*`.
- **B-3 (P1). Ответы не по §4-таблице сверх retry.** Oversize → **400, а не
  413** (`otlp/http.go:97-101` MaxBytesReader→BadRequest; аналогично metric/
  logs; jaeger LimitReader вообще молча усекает). И **нет `partial_success`**:
  при частично-невалидном payload §4 требует `200 + partial_success` (отправитель
  не ретраит), а coral либо принимает всё, либо `400`; невалидные спаны
  `validate` дропает молча (P1-7) без отчёта. Приёмник не реализует
  partial-семантику совсем.
- **B-4 (P1). `service.name` не enforced.** §6: обязателен на каждом ресурсе
  (агрегации amber, матчинг fathom). coral — точка обогащения — его не
  проверяет и не проставляет (`grep` пусто). Спаны без `service.name` уходят
  в стор как есть.
- **B-5 (нит). Example-конфиги не по канонической схеме.** Экспортёр в amber
  указывает `:8080` (`collector.example.yaml:47,83`), а не `5318`; порты
  метрик/логов `4319-4322` — те, что §2 велит свернуть. При фиксе P0-1 и
  §2 обновить example-файлы (правило §9: дефолты кода == example == доки).

### C. Что coral отдаёт в единый контракт
Единый OTLP-эндпоинт 4317/4318 (§2, P0-1); заполнение §4-таблицы на приёме
(P0-3, B-3); §6-гарантия обогащения `k8s.*/cloud.*/collector=coral` + требование
к amber хранить это для всех сигналов (P1-6); §7 средний ярус кардинальности
(Q-2); at-most-once + durability на краях (§1, уже зафиксировано); негарантия
порядка доставки (§4/P2-11) — для wisp-reset и fathom-матчинга.

---

## Улучшения (не баги)

- **Q-1. Дедупликация трёх копий ядра.** `pipeline/pipeline.go` (173),
  `metric/pipeline.go` (182), `logs/pipeline.go` (131) — одна worker-pool/
  queue/retry-механика копипастой, уже разошедшаяся (у логов нет процессоров и
  drop-счётчиков, у метрик нет drop-счётчиков, у трейсов есть). Generic
  `Pipeline[T]` + сигнал-специфичные процессоры. Разблокирует P0-1.
- **Q-2. Метрик-процессоры**: `cardinality_limit` (по контракту «coral —
  страховка») и `batch` (сейчас метрики уходят поштучно от батча приёмника).
- **Q-3. Дизайн-доки уровня wisp** в vault (coral — единственный продукт без
  полного набора; есть только лёгкий `_index.md`).
- **Q-4. e2e в `test/integration`** — расширить на все три сигнала и на
  одновременный fan-out amber+fathom.

## Что должно попасть в единый контракт (вход из coral)
Единый OTLP-эндпоинт 4317/4318 (P0-1); таблица ошибок/ретраев (P0-3);
гарантии обогащения и требование к amber хранить атрибуты для всех сигналов
(P1-6/7); at-most-once внутри coral и durability на краях (нит про спул);
порядок доставки не гарантируется (P2-11) — следствие для wisp-reset и
fathom-матчинга.
