--DROP TABLE IF EXISTS dds.srv_wf_settings;
CREATE TABLE IF NOT EXISTS dds.srv_wf_settings (
    id SERIAL PRIMARY KEY,
    workflow_key VARCHAR(255) NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);