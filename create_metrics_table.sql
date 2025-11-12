-- Script para criar tabela metrics para armazenar métricas diárias dos canais
-- Esta tabela substitui o uso do campo stats_history na tabela channels

CREATE TABLE IF NOT EXISTS public.metrics (
  id bigserial NOT NULL,
  channel_id text NOT NULL,
  date date NOT NULL,
  views bigint NULL DEFAULT 0,
  subscribers bigint NULL DEFAULT 0,
  video_count integer NULL DEFAULT 0,
  created_at timestamp with time zone NULL DEFAULT now(),
  CONSTRAINT metrics_pkey PRIMARY KEY (id),
  CONSTRAINT metrics_channel_id_date_key UNIQUE (channel_id, date),
  CONSTRAINT metrics_channel_id_fkey FOREIGN KEY (channel_id) 
    REFERENCES channels (channel_id) ON DELETE CASCADE
) TABLESPACE pg_default;

-- Índice para melhorar performance de consultas por channel_id
CREATE INDEX IF NOT EXISTS idx_metrics_channel_id ON public.metrics(channel_id);

-- Índice para melhorar performance de consultas por data
CREATE INDEX IF NOT EXISTS idx_metrics_date ON public.metrics(date);

-- Índice composto para consultas por canal e data
CREATE INDEX IF NOT EXISTS idx_metrics_channel_date ON public.metrics(channel_id, date);

-- Comentários para documentação
COMMENT ON TABLE public.metrics IS 'Armazena métricas diárias de cada canal (views, subscribers, video_count)';
COMMENT ON COLUMN public.metrics.channel_id IS 'ID do canal (foreign key para channels.channel_id)';
COMMENT ON COLUMN public.metrics.date IS 'Data da métrica (sem hora)';
COMMENT ON COLUMN public.metrics.views IS 'Total de visualizações do canal na data';
COMMENT ON COLUMN public.metrics.subscribers IS 'Total de inscritos do canal na data';
COMMENT ON COLUMN public.metrics.video_count IS 'Total de vídeos do canal na data';

