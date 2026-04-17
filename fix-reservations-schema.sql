-- ============================================================
-- BUDDICA高松店: tkm_reservations テーブル修正
-- 既存の簡易スキーマ(id + data_json)を完全スキーマに置き換え
-- Supabase SQL Editor で実行
-- ============================================================

-- 1. 既存テーブル削除（データ空なので安全）
DROP TABLE IF EXISTS tkm_fleet;
DROP TABLE IF EXISTS tkm_reservations;

-- 2. 完全スキーマで再作成
CREATE TABLE tkm_reservations (
  id TEXT PRIMARY KEY,
  name TEXT DEFAULT '',
  kana TEXT,
  start_date TEXT DEFAULT '',
  end_date TEXT DEFAULT '',
  start_time TEXT DEFAULT '',
  end_time TEXT DEFAULT '',
  del_time TEXT DEFAULT '',
  col_time TEXT DEFAULT '',
  vehicle_class TEXT DEFAULT '',
  vehicle_name TEXT,
  plate_no TEXT,
  source TEXT,
  status TEXT DEFAULT 'confirmed',
  memo TEXT,
  tel TEXT DEFAULT '',
  mail TEXT DEFAULT '',
  ota TEXT DEFAULT '',
  booking_no TEXT,
  no TEXT,
  people INTEGER DEFAULT 0,
  insurance TEXT DEFAULT '',
  del_place TEXT DEFAULT '',
  col_place TEXT DEFAULT '',
  del_flight TEXT DEFAULT '',
  col_flight TEXT,
  usb TEXT,
  car_seat TEXT DEFAULT '0',
  junior_seat TEXT DEFAULT '0',
  options TEXT,
  amount INTEGER DEFAULT 0,
  price INTEGER DEFAULT 0,
  final_price INTEGER DEFAULT 0,
  line TEXT,
  payment TEXT,
  del_date TEXT,
  del_route TEXT,
  del_memo TEXT,
  col_date TEXT,
  col_route TEXT,
  col_memo TEXT,
  visit_type TEXT DEFAULT '',
  return_type TEXT DEFAULT '',
  assigned_vehicle TEXT DEFAULT '',
  prefecture TEXT DEFAULT '',
  updated_by TEXT DEFAULT '',
  changed_json TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. 配車テーブル再作成
CREATE TABLE tkm_fleet (
  reservation_id TEXT PRIMARY KEY,
  vehicle_code TEXT NOT NULL
);

-- 4. RLS再設定
ALTER TABLE tkm_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_fleet ENABLE ROW LEVEL SECURITY;
CREATE POLICY tkm_reservations_all ON tkm_reservations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_fleet_all ON tkm_fleet FOR ALL USING (true) WITH CHECK (true);
