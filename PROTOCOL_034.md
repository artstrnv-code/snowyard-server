# Snowyard Relay 0.3.4: fresh frozen checkpoint during roster v2

Клиент: 0.12i; `hello.rosterVersion=2`. `rosterVersion=1` работает по старому протоколу.

1. JOINER загружает исходный снимок и догоняет журнал (`sync_loaded`).
2. Relay замораживает комнату и рассылает `roster_prepare` **только текущим ACTIVE**. У сообщения намеренно отсутствует закреплённый старый хеш: клиент голосует `roster_ready` за свой замороженный локальный `world.tick` и fingerprint.
3. Если все ACTIVE подтвердили одинаковые `tick/hash`, Relay отправляет `snapshot_requested {generation,tick}` активному координатору.
4. Координатор захватывает snapshot с **этого же** тика (без дальнейшего шага симуляции) и загружает через `snapshot_begin/chunk/commit`. Тик 0 допустим только для свежего roster snapshot.
5. Relay высылает назначенному JOINER **новый** `snapshot_manifest`. Все ранее отправленные, но ещё не исполненные команды старого поколения поверх границы snapshot не воспроизводятся.
6. JOINER скачивает байты, проверяет SHA-256, восстанавливает мир, выполняет replay и отправляет `sync_finish`.
7. Relay присылает `roster_checkpoint` **только после успешного `sync_finish` этого нового snapshotId**.
8. JOINER сравнивает ровно `world.tick` и gameplay fingerprint, отвечает `roster_join_ready` с теми же `tick/hash`.
9. Relay рассылает `roster_commit`, генерация состава увеличивается. Другие ожидающие JOINER подключаются в последующих транзакциях.

Тайм-аут 120 секунд на всю транзакцию. Автоматический поиск исправного состояния после реального disagreement остаётся не реализован.
