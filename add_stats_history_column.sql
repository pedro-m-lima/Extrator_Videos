-- Script para adicionar coluna stats_history na tabela channels
-- Execute este SQL no Supabase apenas se a coluna não existir

-- Adiciona coluna stats_history se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'channels' AND column_name = 'stats_history'
    ) THEN
        ALTER TABLE channels ADD COLUMN stats_history JSONB DEFAULT '{}'::jsonb;
    END IF;
END $$;

-- Exemplo de estrutura do JSON stats_history:
-- {
--   "2025": {
--     "11": {
--       "10": {
--         "views": 1000000,
--         "subscribers": 50000,
--         "video_count": 200,
--         "timestamp": "2025-11-10T23:59:00.000000"
--       },
--       "11": {
--         "views": 1005000,
--         "subscribers": 50100,
--         "video_count": 201,
--         "timestamp": "2025-11-11T23:59:00.000000"
--       }
--     }
--   }
-- }

