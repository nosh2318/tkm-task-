-- ============================================================
-- BUDDICA高松店 完全スキーマ（buddica-takamatsu-2）
-- Supabase SQL Editor で実行
-- ============================================================

-- 1. tkm_reservations（予約データ — 完全カラム版）
CREATE TABLE IF NOT EXISTS tkm_reservations (
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

-- 2. tkm_fleet（車両配車マッピング）
CREATE TABLE IF NOT EXISTS tkm_fleet (
  reservation_id TEXT PRIMARY KEY,
  vehicle_code TEXT NOT NULL
);

-- 3. tkm_tasks（日次タスク）
CREATE TABLE IF NOT EXISTS tkm_tasks (
  _id TEXT PRIMARY KEY,
  date TEXT NOT NULL,
  type TEXT,
  time TEXT,
  name TEXT,
  assignee TEXT,
  vehicle TEXT,
  place TEXT,
  people INTEGER DEFAULT 0,
  insurance TEXT,
  flight TEXT,
  reservation_id TEXT,
  ota TEXT,
  tel TEXT,
  mail TEXT,
  done BOOLEAN DEFAULT FALSE,
  memo TEXT,
  assigned_vehicle TEXT,
  plate_no TEXT,
  insurance_change TEXT,
  opts_json TEXT,
  changed_json TEXT DEFAULT '{}',
  yakkan BOOLEAN DEFAULT FALSE,
  line BOOLEAN DEFAULT FALSE,
  payment BOOLEAN DEFAULT FALSE,
  return_date TEXT,
  return_time TEXT,
  return_type TEXT,
  col_place TEXT,
  manual BOOLEAN DEFAULT FALSE,
  sort_order INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tkm_tasks_date ON tkm_tasks(date);

-- 4. tkm_staff（スタッフ）
CREATE TABLE IF NOT EXISTS tkm_staff (
  name TEXT PRIMARY KEY,
  type TEXT DEFAULT '正社員',
  memo TEXT DEFAULT '',
  hourly_wage INTEGER DEFAULT 0,
  transport_cost INTEGER DEFAULT 0,
  monthly_salary INTEGER DEFAULT 0,
  sort_order INTEGER DEFAULT 0
);

-- 5. tkm_vehicles（車両マスター）
CREATE TABLE IF NOT EXISTS tkm_vehicles (
  code TEXT PRIMARY KEY,
  name TEXT,
  plate_no TEXT,
  type TEXT,
  seats INTEGER DEFAULT 5,
  active BOOLEAN DEFAULT TRUE
);

-- 6. tkm_shifts（シフト）
CREATE TABLE IF NOT EXISTS tkm_shifts (
  date TEXT NOT NULL,
  staff_name TEXT NOT NULL,
  symbol TEXT DEFAULT '',
  start_time TEXT DEFAULT '',
  end_time TEXT DEFAULT '',
  memo TEXT DEFAULT '',
  status TEXT DEFAULT '確定',
  PRIMARY KEY (date, staff_name)
);

-- 7. tkm_attendance（勤怠）
CREATE TABLE IF NOT EXISTS tkm_attendance (
  date TEXT NOT NULL,
  staff_name TEXT NOT NULL,
  start_time TEXT DEFAULT '',
  end_time TEXT DEFAULT '',
  approved BOOLEAN DEFAULT FALSE,
  memo TEXT DEFAULT '',
  absent BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (date, staff_name)
);

-- 8. tkm_maintenance（メンテナンス）
CREATE TABLE IF NOT EXISTS tkm_maintenance (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  vehicle_code TEXT,
  start_date TEXT,
  end_date TEXT,
  label TEXT
);

-- 9. tkm_app_settings（アプリ設定）
CREATE TABLE IF NOT EXISTS tkm_app_settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

-- 10. tkm_reservation_changes（予約変更履歴）
CREATE TABLE IF NOT EXISTS tkm_reservation_changes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reservation_id TEXT,
  field_name TEXT,
  old_value TEXT,
  new_value TEXT,
  changed_by TEXT DEFAULT 'system',
  changed_at TIMESTAMPTZ DEFAULT now()
);

-- 11. tkm_wage_history（時給変更履歴）
CREATE TABLE IF NOT EXISTS tkm_wage_history (
  id TEXT PRIMARY KEY,
  staff_name TEXT,
  effective_date DATE,
  hourly_wage INTEGER,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 12. tkm_memos（メモ）
CREATE TABLE IF NOT EXISTS tkm_memos (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  content TEXT,
  target_date TEXT,
  done BOOLEAN DEFAULT FALSE,
  image_url TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 13. tkm_accounting（会計）
CREATE TABLE IF NOT EXISTS tkm_accounting (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  date TEXT,
  type TEXT DEFAULT '売上',
  category TEXT DEFAULT '',
  description TEXT DEFAULT '',
  amount INTEGER DEFAULT 0,
  memo TEXT DEFAULT '',
  reservation_id TEXT,
  paid BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 14. tkm_jalan_payment（じゃらん決済）
CREATE TABLE IF NOT EXISTS tkm_jalan_payment (
  reservation_id TEXT PRIMARY KEY,
  category TEXT DEFAULT '売上',
  pay_status TEXT DEFAULT '未完了',
  send_date TEXT DEFAULT '',
  refund_date TEXT DEFAULT '',
  content TEXT DEFAULT '',
  link_url TEXT DEFAULT '',
  adjusted_price TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 15. tkm_cars（BUDDICA Fleet 車両管理）
CREATE TABLE IF NOT EXISTS tkm_cars (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  store_id TEXT DEFAULT 'takamatsu',
  name TEXT DEFAULT '',
  plate TEXT DEFAULT '',
  class_id TEXT DEFAULT '',
  status TEXT DEFAULT 'active',
  memo TEXT DEFAULT '',
  notes TEXT DEFAULT '',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- ===== RLS ポリシー =====
ALTER TABLE tkm_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_fleet ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_staff ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_vehicles ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_shifts ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_attendance ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_maintenance ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_app_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_reservation_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_wage_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_memos ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_accounting ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_jalan_payment ENABLE ROW LEVEL SECURITY;
ALTER TABLE tkm_cars ENABLE ROW LEVEL SECURITY;

CREATE POLICY tkm_reservations_all ON tkm_reservations FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_fleet_all ON tkm_fleet FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_tasks_all ON tkm_tasks FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_staff_all ON tkm_staff FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_vehicles_all ON tkm_vehicles FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_shifts_all ON tkm_shifts FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_attendance_all ON tkm_attendance FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_maintenance_all ON tkm_maintenance FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_app_settings_all ON tkm_app_settings FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_reservation_changes_all ON tkm_reservation_changes FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_wage_history_all ON tkm_wage_history FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_memos_all ON tkm_memos FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_accounting_all ON tkm_accounting FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_jalan_payment_all ON tkm_jalan_payment FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY tkm_cars_all ON tkm_cars FOR ALL USING (true) WITH CHECK (true);

-- ===== Storage バケット =====
INSERT INTO storage.buckets (id, name, public) VALUES ('licenses', 'licenses', true) ON CONFLICT DO NOTHING;
CREATE POLICY licenses_all ON storage.objects FOR ALL USING (bucket_id = 'licenses') WITH CHECK (bucket_id = 'licenses');

-- ===== テスト予約 =====
INSERT INTO tkm_reservations (id, name, start_date, end_date, start_time, end_time, ota, vehicle_class, people, insurance, status, visit_type, return_type, price)
VALUES ('TEST-001', 'テスト太郎', '2026-04-18', '2026-04-19', '09:00', '19:00', 'HP', 'A', 1, 'なし', 'confirmed', '来店', '来店', 5000);
