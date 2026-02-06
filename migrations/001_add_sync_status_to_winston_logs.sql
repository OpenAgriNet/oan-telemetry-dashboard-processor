-- Migration: Add sync_status column to winston_logs table
-- This column is required by oan-telemetry-dashboard-processor to track processed logs
-- Date: 2026-02-06

-- Add sync_status column if it doesn't exist
ALTER TABLE public.winston_logs 
ADD COLUMN IF NOT EXISTS sync_status integer DEFAULT 0;

-- Set all existing rows to sync_status = 0 so they get processed
-- (This is safe because NULL values will be set to 0, and existing 0s remain 0)
UPDATE public.winston_logs 
SET sync_status = 0 
WHERE sync_status IS NULL;

-- Add comment for documentation
COMMENT ON COLUMN public.winston_logs.sync_status IS 'Processing status: 0 = unprocessed, 1 = processed by oan-telemetry-dashboard-processor';
