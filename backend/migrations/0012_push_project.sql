-- FCM project sharding (audit PERF-1). One Firebase project caps FCM HTTP v1 sends at about
-- 600,000 per minute, so Android devices are spread across several projects. An FCM registration
-- token only works with the project it was registered with, so the device row keeps that
-- project's id. NULL means the first (default) configured project, so every existing device
-- keeps working. Only meaningful when push_platform = 'android'.
ALTER TABLE devices ADD COLUMN push_project text;

-- The sender clears a dead push target (clearPushToken), which now also clears the project.
-- 0011 granted column-level UPDATE on the other target columns; table-level SELECT and the
-- API's table-level grants already cover the new column.
GRANT UPDATE (push_project) ON TABLE devices TO relay_sender;
