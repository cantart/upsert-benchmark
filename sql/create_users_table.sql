-- Creates the users table and seeds it with initial benchmark data.
-- Run automatically by the Postgres container when mounted under /docker-entrypoint-initdb.d.

CREATE TABLE IF NOT EXISTS public.users (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_email ON public.users (email);

INSERT INTO public.users (name, email)
SELECT
    'User ' || gs AS name,
    'user' || gs || '@example.com' AS email
FROM generate_series(1, 100) AS gs
ON CONFLICT (email) DO NOTHING;
