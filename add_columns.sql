-- Script para adicionar colunas opcionais na tabela videos
-- Execute este SQL no Supabase apenas se as colunas não existirem

-- Adiciona coluna tags se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'videos' AND column_name = 'tags'
    ) THEN
        ALTER TABLE videos ADD COLUMN tags TEXT;
    END IF;
END $$;

-- Adiciona coluna duration se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'videos' AND column_name = 'duration'
    ) THEN
        ALTER TABLE videos ADD COLUMN duration TEXT;
    END IF;
END $$;

-- Adiciona coluna format se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'videos' AND column_name = 'format'
    ) THEN
        ALTER TABLE videos ADD COLUMN format TEXT DEFAULT '16:9';
    END IF;
END $$;

-- Adiciona coluna is_short se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'videos' AND column_name = 'is_short'
    ) THEN
        ALTER TABLE videos ADD COLUMN is_short BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- Adiciona coluna is_invalid se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'videos' AND column_name = 'is_invalid'
    ) THEN
        ALTER TABLE videos ADD COLUMN is_invalid BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- Adiciona colunas na tabela channels se não existirem
-- Adiciona coluna needs_old_videos se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'channels' AND column_name = 'needs_old_videos'
    ) THEN
        ALTER TABLE channels ADD COLUMN needs_old_videos BOOLEAN DEFAULT TRUE;
    END IF;
END $$;

-- Adiciona coluna priority se não existir
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'channels' AND column_name = 'priority'
    ) THEN
        ALTER TABLE channels ADD COLUMN priority INTEGER DEFAULT 5;
    END IF;
END $$;

