DROP TRIGGER IF EXISTS outbox_events_notify ON outbox_events;
DROP FUNCTION IF EXISTS notify_gateway_event();
DROP TABLE IF EXISTS outbox_events;
