# Listado Unificado de Brechas Conocidas (Bugs) - Aplicación Postnet

---

## AUTHORIZATION - Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Resolver antes de conectar un adquirente real

Con el simulador, estas fallas solo ocurren con [los montos de prueba](#probar-los-fallos-del-adquirente).

1. **El conciliador no consulta el resultado**
   * **Consecuencia:** Revierte toda operación sin respuesta, también las que el emisor aprobó: el cliente ve un rechazo y puede reintentar. El protocolo del host puede ofrecer algo mejor.
   * **Dónde:** `ReconcileUnresolved`
2. **Respuesta tardía al cliente**
   * **Consecuencia:** Sin respuesta del adquirente, el cliente espera hasta `ACQUIRER_STUCK_AFTER` más una corrida. Si Terminal Gateway ya borró la sesión, el rechazo no encuentra a quién avisar.
   * **Dónde:** `ReconcileUnresolved`, `DeleteExpired`
3. **Terminal Gateway no conoce el resultado de una reversa**
   * **Consecuencia:** Ya respondió `202` e ignora `reversal-completed`: el terminal no sabe si se completó ni si se ignoró. Se consulta con `GET /transactions/{id}`. Resolverlo requiere cambiar Terminal Gateway.
   * **Dónde:** `tg_nats_subscriber.go`
4. **Una reversa que nunca se completa no tiene límite**
   * **Consecuencia:** Se reintenta cada corrida, sin un máximo ni una espera creciente, hasta que el host responda. Solo avisa la alerta.
   * **Dónde:** `ReconcileUnresolved`
5. **Workers ocupados frenan todo el consumo**
   * **Consecuencia:** Con un adquirente caído, las llamadas ocupan los workers hasta su timeout, y las demás esperan. Sin un corte, el host caído suma demora a cada venta.
   * **Dónde:** `runAcquirerCall`
6. **Sin migración, el servicio sigue consumiendo**
   * **Consecuencia:** `/readyz` y Docker lo avisan, pero los eventos fallan y, al agotar los reintentos, se pierden.
   * **Dónde:** `wire`

### Saga

7. **No existe el bypass ni el timeout de fraude**
   * **Consecuencia:** El log dice `fraud bypass will apply` y la documentación del evento menciona 500 ms, pero `BypassFraudCheck` no se invoca. Si Fraud Detection no responde, la transacción queda trabada para siempre.
   * **Dónde:** `AuthorizeTransaction`
8. **Resultados publicados sin outbox**
   * **Consecuencia:** Si falla la publicación de una aprobación, Settlement no la liquida y el cajero no la ve, aunque figure `APPROVED`.
   * **Dónde:** `callAcquirer`, `publishRejection`, `reconcile`
9. **Una transacción inválida no recibe respuesta**
   * **Consecuencia:** Terminal Gateway no valida `card_network` ni `card_last4`: `/pay` responde `202`, Authorization descarta el mensaje y la sesión queda en `PROCESSING` hasta su limpieza.
   * **Dónde:** `AuthorizeTransaction`
10. **Un segundo evento con el mismo `transaction_id` reinicia la transacción**
    * **Consecuencia:** `Save` es un UPSERT: un `event_id` distinto la devuelve a `FRAUD_CHECKING` y puede cobrarse otra vez. Hoy Terminal Gateway no genera este caso, pero Authorization no se defiende.
    * **Dónde:** `AuthorizeTransaction`, `TransactionRepo.Save`
11. **`Save` queda fuera de la transacción de idempotencia**
    * **Consecuencia:** Usa el pool en lugar de `dbTx`, aunque el comentario diga lo contrario. Si falla el commit, queda guardado sin el evento marcado y la reentrega reprocesa.
    * **Dónde:** `AuthorizeTransaction`, `rejectBlockedCard`
12. **Notification no consume `reversal-completed`**
    * **Consecuencia:** El contrato del evento prevé un comprobante de anulación, pero el subscriber de Notification no se suscribe, así que no se genera.
    * **Dónde:** `nt_nats_subscriber.go`

### Blocklist y datos de tarjeta

13. **Solo tarjetas propias**
    * **Consecuencia:** El PAN lo resolverá `card-management-service`, que solo conoce las tarjetas del emisor propio. Una tarjeta de otro banco se rechaza con `14`. Procesarlas requiere otra implementación de `CardDataProvider`.
    * **Dónde:** `CardDataProvider`
14. **Sin TLS hacia el host**
    * **Consecuencia:** El mensaje lleva el PAN en claro por TCP. Solo se puede usar con tarjetas de prueba.
    * **Dónde:** `iso8583.Client`
15. **Dialecto propio**
    * **Consecuencia:** Los campos siguen ISO 8583:1987, pero un host real va a pedir los suyos, además de certificación.
    * **Dónde:** `messages.go`
16. **El stub no usa `card_token` para nada real**
    * **Consecuencia:** Devuelve una tarjeta de prueba: no valida que el token exista ni que coincidan los últimos 4 dígitos.
    * **Dónde:** `carddata.Stub`
17. **Sin caso de uso para desbloquear**
    * **Consecuencia:** Un bloqueo es permanente, salvo que se borre la fila a mano.
    * **Dónde:** `BlockedCardRepository`
18. **`iso8583_raw` se guarda tal como llega**
    * **Consecuencia:** El mensaje al host no se guarda ahí, pero si un terminal lo enviara con el PAN, quedaría en la base. Definir qué se guarda antes de recibir mensajes de terminales.
    * **Dónde:** `TransactionRepo.Save`

### Operación y datos

19. **Sin migraciones automáticas**
    * **Consecuencia:** Cambios como `card_token` requieren aplicarse a mano; si no, fallan todas las transacciones.
    * **Dónde:** `pkg/pgutil/migrate.go`
20. **`fraud_rules_hit` nunca se guarda**
    * **Consecuencia:** `Save` no incluye la columna: las reglas que motivaron una decisión se pierden para auditoría.
    * **Dónde:** `TransactionRepo.Save`
21. **Faltan índices en `01-init.sql`**
    * **Consecuencia:** Solo está el de `card_token`. Las consultas de historial de Fraud Detection, por terminal y fecha, recorren la tabla.
    * **Dónde:** `01-init.sql`
22. **Acoplamiento con Fraud Detection**
    * **Consecuencia:** Lee las tablas de este contexto directamente, en contra de lo que indica la documentación de arquitectura.
    * **Dónde:** `fd_postgres_repository.go`
23. **gRPC sin TLS, sin autenticación y sin apagado ordenado**
    * **Consecuencia:** Si falla el listen, solo se registra el error y el proceso sigue. El puerto ignora `GRPC_PORT`.
    * **Dónde:** `server.go`, `wire.go`
24. **API HTTP sin autenticación**
    * **Consecuencia:** Cualquiera con acceso de red y un ID puede leer el estado y el monto de una transacción.
    * **Dónde:** `router.go`
25. **`/readyz` no verifica NATS**
    * **Consecuencia:** Una instancia sin NATS sigue figurando como lista.
    * **Dónde:** `handler.go`
26. **La fuente del rechazo no se reconstruye bien**
    * **Consecuencia:** `BLOCKLIST` y `VALIDATION` se leen como `ACQUIRER`. Hoy no tiene efecto, porque un rechazo no se vuelve a guardar.
    * **Dónde:** `reconstitute.go`
27. **Código sin uso**
    * **Consecuencia:** `FindBySTAN`, `UpdateState`, `ExistsByID`, `parseRFC3339` y los eventos de dominio internos no se usan.
    * **Dónde:** `repository.go`, `server.go`, `transaction.go`

---

## FRAUD-DETECTION - Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Efectividad del control de fraude

28. **`REJECT` es inalcanzable en el flujo QR**
    * **Consecuencia:** Con los pesos por defecto, el fraude no puede rechazar ninguna transacción de ese flujo.
    * **Dónde:** `fraud_rules`, `RULE-003`
29. **Las fallas de historial se ocultan**
    * **Consecuencia:** Una base lenta o caída produce `APPROVE` con score 0, sin log ni error. Solo los timeouts dejan una métrica.
    * **Dónde:** `RuleEngine.Evaluate`, `buildRuleContext`
30. **Sin índices para el historial**
    * **Consecuencia:** A medida que crece la tabla, las consultas pueden superar los 200 ms y activar la falla anterior.
    * **Dónde:** `01-init.sql`
31. **`threshold_value` no se usa**
    * **Consecuencia:** `PUT /rules/{id}/threshold` responde `updated` y `GET /rules` muestra el valor nuevo, pero las condiciones siguen fijas.
    * **Dónde:** `registerEvalFns`
32. **Los umbrales de decisión no se usan**
    * **Consecuencia:** `ENGINE_SCORE_THRESHOLD_*` se muestran en el log de arranque, pero `NewFraudScore` usa 50 y 70 fijos.
    * **Dónde:** `NewFraudScore`, `fd_main.go`
33. **`REVIEW` no genera ninguna revisión**
    * **Consecuencia:** La transacción continúa y solo queda registrada; no hay cola ni proceso.
    * **Dónde:** Authorization, `fraud_cases`
34. **Las reglas no distinguen moneda**
    * **Consecuencia:** `RULE-002` mezcla monedas en el promedio y `RULE-005` usa un límite en ARS para cualquier moneda.
    * **Dónde:** `fd_postgres_repository.go`, `registerEvalFns`
35. **Sin reglas por tarjeta**
    * **Consecuencia:** El evento no incluye datos de la tarjeta, así que no se puede detectar el uso repetido de una misma tarjeta.
    * **Dónde:** `FraudCheckRequestedPayload`

### Confiabilidad de la Saga

36. **El score se publica sin outbox**
    * **Consecuencia:** Si falla o el proceso cae, Authorization queda en `FRAUD_CHECKING`. El log dice `authorization will apply bypass`, pero ese mecanismo no existe.
    * **Dónde:** `EvaluateTransaction`
37. **Un evento inválido no recibe respuesta**
    * **Consecuencia:** Se descarta sin publicar un score, y Authorization queda esperando.
    * **Dónde:** `EvaluateTransaction`
38. **El score neutral no se guarda ni marca el evento**
    * **Consecuencia:** No queda caso auditable: `GET /fraud-cases` devuelve `404`, y una reentrega puede publicar un segundo score con otro `event_id`.
    * **Dónde:** `EvaluateTransaction`
39. **Sin reglas activas, todo es `REVIEW`**
    * **Consecuencia:** Desactivar todas las reglas no apaga el motor: cada transacción pasa con score neutral.
    * **Dónde:** `RuleEngine.Evaluate`
40. **El motor corre antes de la idempotencia**
    * **Consecuencia:** Una reentrega repite las consultas y duplica las métricas de evaluación. El comentario del handler describe el orden inverso.
    * **Dónde:** `EvaluateTransaction`
41. **`Save` queda fuera de la transacción**
    * **Consecuencia:** Usa el pool en lugar de `tx`. Hoy es inocuo gracias a `ON CONFLICT DO NOTHING`, pero contradice el comentario de que es atómico.
    * **Dónde:** `EvaluateTransaction`

### Seguridad y administración

42. **La API de reglas no tiene autenticación**
    * **Consecuencia:** Con acceso de red, cualquiera puede bajar todos los pesos a 1 y anular el control de fraude. Tampoco queda registro de quién hizo el cambio.
    * **Dónde:** `fd_http_router.go`, `fd_admin_handler.go`
43. **El cambio de reglas modifica la caché antes de guardar**
    * **Consecuencia:** La regla es un puntero compartido con evaluaciones en curso: es una condición de carrera. Si `Save` falla, la instancia queda con un peso que la base no tiene.
    * **Dónde:** `UpdateRuleThreshold`
44. **Sin endpoint para activar o desactivar reglas**
    * **Consecuencia:** `Deactivate()` existe, pero no se invoca: hay que cambiar la tabla a mano.
    * **Dónde:** `fd_fraud_rule.go`

### Operación y datos

45. **Acceso directo a tablas de Authorization**
    * **Consecuencia:** Un cambio de columnas allí rompe las reglas, sin error de compilación. El fallo además queda oculto por la brecha de historial.
    * **Dónde:** `TransactionHistoryRepo`
46. **`rule_name` no se guarda**
    * **Consecuencia:** `RuleName` llega vacío en `GET /fraud-cases`.
    * **Dónde:** `FraudCaseRepo.Save`
47. **Sin migraciones reales e índices faltantes**
    * **Consecuencia:** Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `fd_schema.sql` difieren.
    * **Dónde:** `pkg/pgutil/migrate.go`
48. **Sin dashboards de decisión ni alertas de negocio**
    * **Consecuencia:** Hay métricas de decisiones y reglas activadas, pero ningún dashboard las muestra ni hay alertas sobre ellas.
    * **Dónde:** Grafana, Prometheus
49. **gRPC placeholder y sin apagado ordenado**
    * **Consecuencia:** Abre un puerto que no ofrece nada y que ignora `GRPC_PORT`.
    * **Dónde:** `fd_grpc_server.go`, `fd_wire.go`
50. **`/readyz` no verifica NATS**
    * **Consecuencia:** Una instancia sin NATS sigue figurando como lista.
    * **Dónde:** `fd_http_handler.go`
51. **Referencias a un timeout de 500 ms**
    * **Consecuencia:** La validación de `ENGINE_EVAL_TIMEOUT` y varios comentarios lo mencionan, pero Authorization no tiene ese timeout.
    * **Dónde:** `fd_config.go`
52. **Código sin uso**
    * **Consecuencia:** `port.FraudService` y `port.AdminService` no se implementan; tampoco se usan `parseTerminalID` ni los eventos de dominio internos.
    * **Dónde:** `fd_port_inbound.go`, `fd_rule_engine.go`, `fd_fraud_case.go`

---

## NOTIFICATION - Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Entrega

53. **Los comprobantes al terminal figuran `SENT` sin entregarse**
    * **Consecuencia:** El historial y las métricas indican éxito, aunque ningún terminal reciba nada.
    * **Dónde:** `tg_grpc_server.go`, `dispatch`
54. **Las notificaciones en `PENDING` nunca se reintentan**
    * **Consecuencia:** Una caída durante el envío o un fallo al guardar las deja perdidas, sin error visible.
    * **Dónde:** `FindPendingRetries`, `dispatch`
55. **`force-retry` no saca de `DEAD`**
    * **Consecuencia:** Responde `202` y puede entregar, pero el estado no cambia y sigue en la lista.
    * **Dónde:** `ForceRetry`, `CanTransitionTo`
56. **`force-retry` acepta cualquier estado**
    * **Consecuencia:** Puede reenviar comprobantes ya entregados.
    * **Dónde:** `ForceRetry`
57. **`SendReceipt` no tiene timeout**
    * **Consecuencia:** Si Terminal Gateway deja de responder, las goroutines quedan esperando indefinidamente.
    * **Dónde:** `dispatch`, `nt_grpc_client.go`
58. **Envíos duplicados en reintentos**
    * **Consecuencia:** El job no reserva lo que procesa: con varias instancias, o un envío lento, se repite la entrega.
    * **Dónde:** `runRetryJob`
59. **Reintentos sin distinguir el error**
    * **Consecuencia:** Un `4xx` permanente o una URL ausente consumen igual los 5 intentos.
    * **Dónde:** `dispatch`
60. **Goroutines sin límite ni espera al apagar**
    * **Consecuencia:** Pueden acumularse envíos, y al apagar se interrumpen sin guardar.
    * **Dónde:** `dispatch`, `runRetryJob`, `close`

### Webhooks y seguridad

61. **Una sola URL para todos los comercios**
    * **Consecuencia:** Los datos de cada comercio llegan al mismo destino. Es un riesgo de exposición si se usa con más de un comercio.
    * **Dónde:** `resolveEndpoint`
62. **Webhooks sin firma**
    * **Consecuencia:** El receptor no puede validar el origen ni la integridad.
    * **Dónde:** `Dispatch`
63. **API sin autenticación ni auditoría**
    * **Consecuencia:** Con acceso de red, cualquiera puede leer notificaciones y disparar reenvíos, sin dejar registro.
    * **Dónde:** `nt_http_handler.go`
64. **gRPC sin TLS**
    * **Consecuencia:** Los comprobantes viajan sin cifrar, y `GRPC_CLIENT_TLS_*` no tienen efecto.
    * **Dónde:** `nt_grpc_client.go`

### Contenido de las notificaciones

65. **El cierre de lote omite discrepancias, cantidad, fecha y terminal**
    * **Consecuencia:** El comercio recibe un monto, pero no puede saber si el lote cerró con diferencias.
    * **Dónde:** `NotifyBatchClosed`
66. **No se generan comprobantes de reversa**
    * **Consecuencia:** El contrato de `reversal-completed` lo prevé, pero no hay suscripción: ni terminal ni comercio se enteran.
    * **Dónde:** `nt_nats_subscriber.go`
67. **`capture_card` se ignora**
    * **Consecuencia:** `pkg/domain/card_capture.go` prevé escalar la alerta, pero no ocurre, y el comprobante no lo indica.
    * **Dónde:** `NotifyRejection`
68. **Los rechazos no generan webhook**
    * **Consecuencia:** Es una decisión explícita del código; el comercio no recibe un aviso automático.
    * **Dónde:** `NotifyRejection`
69. **Comprobante incompleto**
    * **Consecuencia:** `MerchantName` y `MerchantAddress` están vacíos, `TerminalCode` guarda un UUID, y el cliente gRPC omite `transaction_at`.
    * **Dónde:** `NotifyApproval`, `nt_grpc_client.go`
70. **No hay aviso por `settlement.completed`**
    * **Consecuencia:** Settlement lo publica, pero Notification no se suscribe.
    * **Dónde:** `nt_nats_subscriber.go`

### Operación y datos

71. **Sin métrica ni alerta para `DEAD` o `PENDING`**
    * **Consecuencia:** Las pérdidas solo se detectan consultando la base.
    * **Dónde:** `metrics.go`, Prometheus
72. **La API no muestra los intentos**
    * **Consecuencia:** `Attempts` siempre llega en `null`; el motivo del fallo solo se ve por SQL.
    * **Dónde:** `toResult`
73. **`webhook-mock` sin respuesta configurada**
    * **Consecuencia:** En un entorno recién levantado, los webhooks fallan hasta llegar a `DEAD`.
    * **Dónde:** `docker-compose.yml`
74. **Sin índices secundarios**
    * **Consecuencia:** El job y las consultas recorren tablas que crecen indefinidamente.
    * **Dónde:** `01-init.sql`
75. **`dispatched` se publica antes de guardar**
    * **Consecuencia:** Puede anunciar una entrega no registrada.
    * **Dónde:** `dispatch`
76. **Notificaciones huérfanas en reentregas**
    * **Consecuencia:** Un fallo parcial al guardar una aprobación puede dejar filas en `PENDING` y crear otras nuevas.
    * **Dónde:** `NotifyApproval`
77. **Trazas cortadas en el envío**
    * **Consecuencia:** Las goroutines usan `context.Background()`, por lo que el envío queda fuera de la traza del evento.
    * **Dónde:** `NotifyApproval`, `NotifyRejection`, `NotifyBatchClosed`
78. **Nombres de consumidores desactualizados**
    * **Consecuencia:** `consumers.go` menciona durables que no existen.
    * **Dónde:** `pkg/natsutil/consumers.go`
79. **Métrica mal nombrada**
    * **Consecuencia:** `posnet_grpc_request_duration` también mide webhooks.
    * **Dónde:** `metrics.go`
80. **Sin migraciones reales**
    * **Consecuencia:** Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `nt_schema.sql` difieren.
    * **Dónde:** `pkg/pgutil/migrate.go`
81. **gRPC placeholder y sin apagado ordenado**
    * **Consecuencia:** Abre un puerto que no ofrece nada, y `notification.proto` no se usa.
    * **Dónde:** `nt_grpc_server.go`, `nt_wire.go`
82. **`/readyz` no verifica NATS ni Terminal Gateway**
    * **Consecuencia:** Una instancia sin esas dependencias sigue figurando como lista.
    * **Dónde:** `nt_http_handler.go`
83. **Código sin uso**
    * **Consecuencia:** `port.NotificationService`, `port.AdminService`, `ReceiptBuilder` y los eventos de dominio internos no se usan.
    * **Dónde:** `nt_port_inbound.go`, `nt_ports.go`, `nt_notification.go`

---

## SETTLEMENT - Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Montos

84. **Reversas anteriores sin recuperar**
    * **Consecuencia:** El comando las registra, pero hay que ejecutarlo en cada entorno, con aprobación.
    * **Dónde:** Escenario E (Batch close failure)
85. **Un lote retenido no se libera solo**
    * **Consecuencia:** Si la revisión se cierra sola porque la compra llegó tarde, o la cierra un operador, el lote sigue en `DISPUTED` hasta un `resolve-dispute` o `reopen`.
    * **Dónde:** `ProcessBatchClose`
86. **Excluir una reversa no es reversible**
    * **Consecuencia:** Si después llega su compra, la venta suma sin descuento: no queda revisión que la compare.
    * **Dónde:** `EXCLUDE`
87. **Sin corrección en un lote ya enviado**
    * **Consecuencia:** `USE_PURCHASE_AMOUNT` y `EXCLUDE` no aplican a un lote `SUBMITTED` o `SETTLED`: el ajuste se hace con el procesador.
    * **Dónde:** `applyReviewAction`
88. **`batch_date` no valida días anteriores con precisión**
    * **Consecuencia:** Dentro de `SETTLEMENT_CLOSE_MAX_AGE_DAYS`, un terminal puede pedir el cierre de cualquier día con un lote `OPEN`. Si los totales no coinciden, da `DISPUTED`.
    * **Dónde:** `closeDate`
89. **El cambio de zona no mueve lotes**
    * **Consecuencia:** Los lotes ya creados conservan su fecha, y la venta siguiente puede abrir otro lote.
    * **Dónde:** `SetMerchantTimezone`
90. **Comercios con medianoche antes de `SETTLEMENT_BATCH_CLOSE_HOUR`**
    * **Consecuencia:** Sus lotes se cierran a esa hora UTC y pueden disparar `LotesAbiertosVencidos`. Con el valor 3, afecta a UTC-1 y UTC-2.
    * **Dónde:** `runBatchCloser`

### Ciclo de vida del lote

91. **No hay procesador real**
    * **Consecuencia:** `mock` marca `SUBMITTED` sin enviar nada. Falta el adaptador y el archivo de remesa, que requieren la especificación del procesador. `SETTLEMENT_SUBMIT_RETRIES` y `SETTLEMENT_SUBMIT_TIMEOUT` siguen sin efecto.
    * **Dónde:** `st_mock_processor.go`
92. **La confirmación de liquidación es manual**
    * **Consecuencia:** Un operador debe llamar a `settle`, sin una referencia del procesador.
    * **Dónde:** `SettleBatch`
93. **Nadie consume `settlement.completed`**
    * **Consecuencia:** El comercio no recibe el neto liquidado: Notification no se suscribe.
    * **Dónde:** `nt_nats_subscriber.go`
94. **Evento adicional por lotes tardíos**
    * **Consecuencia:** Un lote abierto después de la liquidación diaria genera otro evento con el total acumulado.
    * **Dónde:** `enqueueCompletedIfDone`
95. **El cierre automático no concilia**
    * **Consecuencia:** Los lotes se envían con los totales del backend, sin comparar con el terminal.
    * **Dónde:** `CloseStaleBatches`
96. **`force-close` no concilia**
    * **Consecuencia:** El operador asume que los totales del backend son correctos.
    * **Dónde:** `ForceClose`
97. **Doble envío posible**
    * **Consecuencia:** Si el procesador acepta y falla el guardado o la auditoría, el lote queda `CLOSED`, y `resubmit` lo enviaría otra vez. En `resubmit` queda el log `accepted by processor but not saved` con el `confirmation_id`.
    * **Dónde:** `submitBatch`, `ResubmitBatch`
98. **`resolve-dispute` no notifica**
    * **Consecuencia:** Si los totales cambian, Notification solo conoce los del primer cierre.
    * **Dónde:** `ResolveDispute`
99. **La auditoría no tiene API ni protección propia**
    * **Consecuencia:** Se consulta por SQL, y quien tenga acceso de escritura a la base puede modificarla.
    * **Dónde:** `batch_audit_log`
100. **`force-close`, `resubmit` y `settle` no piden motivo**
     * **Consecuencia:** Su auditoría queda sin `reason`. Exigirlo cambiaría el contrato de la API.
     * **Dónde:** `st_http_handler.go`
101. **Lotes vacíos en `CLOSED`**
     * **Consecuencia:** Los lotes vacíos que cierra el job quedan en `CLOSED` para siempre y aparecen en la API. No cuentan para las alertas.
     * **Dónde:** `autoCloseBatch`

### Seguridad

102. **`SETTLEMENT_ADMIN_API_KEY` no identifica**
     * **Consecuencia:** Mientras esté habilitada, quien la tenga puede hacer todo, declarando cualquier `operator_id`. Hay que crear los tokens y quitarla en cada entorno.
     * **Dónde:** `TokenStore`
103. **Los roles son fijos**
     * **Consecuencia:** No se puede limitar un token a un comercio ni cambiar qué permite cada rol sin tocar el código.
     * **Dónde:** `requiredRole`
104. **Las lecturas no se auditan**
     * **Consecuencia:** El log registra la ruta, pero no quién consultó.
     * **Dónde:** `authMiddleware`
105. **`/metrics` es público**
     * **Consecuencia:** Con acceso de red, se ven contadores y lotes por estado, sin datos de comercios. Hay que limitar el acceso al puerto.
     * **Dónde:** `publicPaths`
106. **El límite de intentos es por instancia**
     * **Consecuencia:** Vive en memoria: se reinicia con el servicio y no se comparte entre instancias.
     * **Dónde:** `authLimiter`
107. **Vencimientos sin aviso**
     * **Consecuencia:** Un token vencido responde `401`, sin un aviso previo ni una alerta.
     * **Dónde:** `TokenStore`
108. **Los certificados requieren reiniciar**
     * **Consecuencia:** No se recargan solos, como los tokens.
     * **Dónde:** `st_wire.go`
109. **Solo cubre la API de Settlement**
     * **Consecuencia:** Los otros BC no cambian, y Postgres y NATS tienen su propia configuración de acceso.
     * **Dónde:** `authMiddleware`

### Operación y datos

110. **Las alertas no avisan a nadie**
     * **Consecuencia:** Alertmanager las envía a `webhook-mock`: hay que mirarlas en Prometheus o Alertmanager.
     * **Dónde:** Alertmanager
111. **Costo creciente por aprobación**
     * **Consecuencia:** Cada venta carga y reinserta todas las transacciones del lote. Los índices evitan recorrer toda la tabla, pero el costo sigue creciendo con el tamaño del lote.
     * **Dónde:** `Save`, `loadTransactions`
112. **Los eventos publicados no llevan la traza**
     * **Consecuencia:** El Relay publica sin el contexto del pedido: la traza del cierre no continúa en Notification. Se correlaciona por `correlation_id`.
     * **Dónde:** `pkg/outbox`
113. **Una fila que no se puede publicar frena a las demás**
     * **Consecuencia:** El ciclo se revierte ante el primer fallo. Hoy solo ocurre si NATS o el stream no están disponibles.
     * **Dónde:** `pkg/outbox`
114. **Un cierre sin lote no recibe respuesta**
     * **Consecuencia:** Terminal Gateway ya respondió `202`, y Settlement solo deja un `WARN`.
     * **Dónde:** `ProcessBatchClose`
115. **Sin DLQ ni reintento pasados unos 12 minutos**
     * **Consecuencia:** Una caída más larga agota las 5 entregas: el evento queda sin registrar hasta que alguien lo reprocesa.
     * **Dónde:** `nak`
116. **Un reintento puede llegar después del cierre**
     * **Consecuencia:** Si una venta está esperando su reentrega cuando el terminal cierra, el cierre da `DISPUTED` y la venta entra en un lote nuevo.
     * **Dónde:** `retryDelays`
117. **Sin migraciones automáticas**
     * **Consecuencia:** Los cambios de schema hay que aplicarlos a mano en las bases existentes.
     * **Dónde:** `pkg/pgutil/migrate.go`
118. **gRPC placeholder**
     * **Consecuencia:** Abre un puerto que no ofrece nada.
     * **Dónde:** `st_grpc_server.go`
119. **Código sin uso**
     * **Consecuencia:** `port.BatchService` y `port.AdminService` no se implementan; tampoco se usan `mdrPercent`, `BatchTxOffline` ni los eventos de dominio internos.
     * **Dónde:** `st_port_inbound.go`, `st_batch_handler.go`, `st_settlement_batch.go`

---

## TERMINAL GATEWAY - Brechas conocidas

Están agrupadas por tema y ordenadas por impacto dentro de cada grupo.

### Seguridad: resolver antes de exponer el servicio

120. **La API HTTP no tiene autenticación y `/pay` es público**
     * **Consecuencia:** Cualquiera con acceso de red y un ID de sesión puede iniciar un pago.
     * **Dónde:** `tg_http_router.go`, `tg_qr_handler.go`
121. **`X-Terminal-ID` es un header que envía el cliente**
     * **Consecuencia:** Permite cancelar o revertir como otro terminal; el mTLS que debería respaldarlo no existe.
     * **Dónde:** `tg_http_handler.go`
122. **CORS `*`**
     * **Consecuencia:** Cualquier sitio web puede llamar a la API desde un navegador.
     * **Dónde:** `tg_http_router.go`
123. **gRPC sin TLS**
     * **Consecuencia:** Tráfico interno sin cifrar ni autenticar.
     * **Dónde:** `tg_grpc_server.go`
124. **Los `500` de `create` y `pay` devuelven `err.Error()`**
     * **Consecuencia:** Se exponen detalles internos al cliente.
     * **Dónde:** `tg_qr_handler.go`

### Pagos y consistencia

125. **El job borra sesiones vencidas en `PROCESSING`**
     * **Consecuencia:** Una aprobación tardía no encuentra la sesión y se reintenta hasta agotar entregas: el pago puede estar aprobado sin que el cajero lo vea.
     * **Dónde:** `DeleteExpired`
126. **La reversa no verifica el estado**
     * **Consecuencia:** Se puede pedir la reversa de una sesión no aprobada, con `original_auth_code` vacío.
     * **Dónde:** `RequestReversal`
127. **Un terminal puede tener varias sesiones activas**
     * **Consecuencia:** Nada impide la creación, aunque el diseño pide una sola; `GetActiveSession` devuelve una cualquiera.
     * **Dónde:** `CreateSession`, `FindActiveByTerminal`
128. **El guardado de la sesión queda fuera de la transacción de idempotencia**
     * **Consecuencia:** `Save` usa el pool en lugar de `tx`: si falla el commit, la sesión queda guardada sin el evento marcado.
     * **Dónde:** `ApplyApproval`, `ApplyRejection`
129. **La transición se valida antes que la idempotencia**
     * **Consecuencia:** Un evento duplicado falla con un error genérico y agota los reintentos en lugar de confirmarse.
     * **Dónde:** `ApplyApproval`, `ApplyRejection`

### Funcionalidad incompleta

130. **`MockNotifier` en lugar de WebSocket**
     * **Consecuencia:** Ningún terminal recibe el resultado.
     * **Dónde:** `tg_wire.go`
131. **La orden de retener la tarjeta no llega al terminal**
     * **Consecuencia:** Solo queda en los logs; tampoco aparece en las respuestas HTTP.
     * **Dónde:** `mock_notifier.go`, `tg_qr_handler.go`
132. **`SendReceipt` responde `delivered=true` sin entregar**
     * **Consecuencia:** Notification puede registrar como entregado un comprobante que no se envió.
     * **Dónde:** `tg_grpc_server.go`
133. **`GetTerminalStatus` no mide la conexión**
     * **Consecuencia:** Un terminal conectado sin sesión figura como `DISCONNECTED`.
     * **Dónde:** `tg_grpc_server.go`
134. **Sin parsing de ISO 8583**
     * **Consecuencia:** `iso8583_raw` y `emv_data_b64` viajan vacíos.
     * **Dónde:** `tg_qr_handler.go`
135. **Sin notificación ni estado al expirar**
     * **Consecuencia:** `NotifySessionExpired` no se invoca y `EXPIRED` no se persiste.
     * **Dónde:** `tg_wire.go`

### Operación

136. **Si falla el listen de gRPC, el proceso sigue y el apagado se cuelga**
     * **Consecuencia:** Solo se registra el error; `GracefulStop` espera indefinidamente a `ready`. El comentario del código asume que `wire()` lo evita, pero no lo hace.
     * **Dónde:** `tg_grpc_server.go`, `tg_wire.go`
137. **El Relay no propaga el trace context**
     * **Consecuencia:** La traza se corta entre TG y Authorization en `transaction.received`.
     * **Dónde:** `pkg/outbox/outbox.go`
138. **Variables de configuración sin efecto**
     * **Consecuencia:** Cambiarlas no tiene ningún resultado.
     * **Dónde:** `tg_wire.go`, `tg_payment_session.go`
139. **Sin migraciones reales e índices faltantes**
     * **Consecuencia:** Los cambios de schema no se aplican a bases existentes, y `01-init.sql` y `tg_schema.sql` difieren.
     * **Dónde:** `pkg/pgutil/migrate.go`
140. **Errores de negocio devueltos como `500`**
     * **Consecuencia:** Falsean las métricas de error y dificultan el manejo en el cliente.
     * **Dónde:** `tg_http_handler.go`, `tg_qr_handler.go`
141. **`/readyz` no verifica NATS**
     * **Consecuencia:** Una instancia sin NATS sigue recibiendo tráfico; los pagos quedan acumulados en el outbox.
     * **Dónde:** `tg_http_handler.go`