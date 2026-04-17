// ============================================================
// GAS - Reservation Email Import & Vehicle Auto-Assignment
// Gmail: reserve@rent-handyman.jp
// Target: 那覇空港 (NHA) store only
// OTA: 楽天(R), じゃらん(J), skyticket(S), エアトリ(O), オフィシャル(HP)
// ============================================================

// --- Config (ScriptProperties から取得) ---
var LABEL_NAME = 'processed_naha';
function getSupabaseUrl_() { return PropertiesService.getScriptProperties().getProperty('SUPABASE_URL'); }
function getSupabaseKey_() { return PropertiesService.getScriptProperties().getProperty('SUPABASE_KEY'); }
function getSlackEmail_() { return PropertiesService.getScriptProperties().getProperty('SLACK_EMAIL'); }

// 初回セットアップ用（1回だけ実行）
function setupProperties() {
  PropertiesService.getScriptProperties().setProperties({
    'SUPABASE_URL': 'https://ckrxttbnawkclshczsia.supabase.co',
    'SUPABASE_KEY': '<SERVICE_ROLE_KEYをSupabase Dashboard > Settings > APIから取得して入力>',
    'SLACK_EMAIL': 'x-aaaatppttzyrldnhjt5el4jj3i@gl-oke5175.slack.com'
  });
  Logger.log('✅ Properties set. setupProperties内のハードコード値を削除してください。');
}

// --- OTA sender definitions ---
var OTA_SENDERS = {
  jalan:     'info@jalan-rentacar.jalan.net',
  rakuten:   'travel@mail.travel.rakuten.co.jp',
  skyticket: 'rentacar@skyticket.com',
  airtrip:   'info@rentacar-mail.airtrip.jp',
  airtrip_dp: 'info@skygate.co.jp',
  official:  'noreply@rent-handyman.jp'
};

// --- OTA reservation subject patterns ---
var OTA_RESERVE_SUBJECTS = {
  jalan:     'じゃらんnetレンタカー 予約通知',
  rakuten:   '【楽天トラベル】予約受付のお知らせ',
  skyticket: '【skyticket】 新規予約',
  airtrip:   '【予約確定】エアトリレンタカー',
  airtrip_dp: '【予約確定】エアトリプラス',
  official:  'ご予約完了のお知らせ'
};

// --- Cancellation keywords in subject ---
var CANCEL_KEYWORDS = ['予約キャンセル受付', 'キャンセル'];

// ============================================================
// Setup & Trigger
// ============================================================
function setup() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'processNewEmails') {
      ScriptApp.deleteTrigger(t);
    }
  });

  ScriptApp.newTrigger('processNewEmails')
    .timeBased()
    .everyMinutes(15)
    .create();

  getOrCreateLabel_(LABEL_NAME);
  Logger.log('Setup complete: 15-minute trigger created, label "' + LABEL_NAME + '" ensured.');
}

// ============================================================
// Main Entry Points
// ============================================================
function processNewEmails() {
  var label = getOrCreateLabel_(LABEL_NAME);
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') -label:' + LABEL_NAME + ' -label:処理済み newer_than:2d';

  var threads = GmailApp.search(query, 0, 50);
  if (threads.length === 0) {
    Logger.log('No new reservation emails found.');
    return;
  }

  Logger.log('Found ' + threads.length + ' thread(s) to process.');

  var successes = [];
  var failures = [];
  var cancellations = [];
  var skipped = [];

  // 全メッセージを時系列順に収集（新規→CXL→取り直しの順序を保証）
  var allMessages = [];
  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();
    for (var j = 0; j < messages.length; j++) {
      allMessages.push({msg: messages[j], thread: threads[i]});
    }
  }
  allMessages.sort(function(a, b) { return a.msg.getDate().getTime() - b.msg.getDate().getTime(); });

  var labeledThreads = {};
  for (var i = 0; i < allMessages.length; i++) {
    try {
      var result = processMessage_(allMessages[i].msg, false);
      if (result) {
        if (result.type === 'success') successes.push(result);
        else if (result.type === 'failure') failures.push(result);
        else if (result.type === 'cancel') cancellations.push(result);
        else if (result.type === 'skip') skipped.push(result);
      }
    } catch (e) {
      Logger.log('ERROR processing message ID ' + allMessages[i].msg.getId() + ': ' + e.message + '\n' + e.stack);
      failures.push({id: '不明', ota: '?', name: '', reason: 'エラー: ' + e.message});
    }
    var tid = allMessages[i].thread.getId();
    if (!labeledThreads[tid]) {
      allMessages[i].thread.addLabel(label);
      labeledThreads[tid] = true;
    }
  }

  if (successes.length > 0) sendSlackSuccess_(successes);
  if (failures.length > 0) sendSlackFailure_(failures);
  if (cancellations.length > 0) sendSlackCancel_(cancellations);

  // ハートビート: 実行完了をDBに記録
  updateHeartbeat_('nha_gas_email', {
    success: successes.length,
    failure: failures.length,
    cancel: cancellations.length,
    skip: skipped.length
  });

  // 未知送信元チェック: reserve@宛の予約系メールでOTA未登録の送信元を検知
  checkUnknownSenders_();
}

function testProcessLatest() {
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') newer_than:7d';
  var threads = GmailApp.search(query, 0, 10);
  if (threads.length === 0) {
    Logger.log('No emails found for test.');
    return;
  }
  Logger.log('[TEST] Found ' + threads.length + ' thread(s).');
  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();
    for (var j = 0; j < messages.length; j++) {
      try {
        processMessage_(messages[j], true);
      } catch (e) {
        Logger.log('[TEST] ERROR: ' + e.message + '\n' + e.stack);
      }
    }
  }
}

// ============================================================
// Message Router
// ============================================================
function processMessage_(message, dryRun) {
  var from = message.getFrom();
  var subject = message.getSubject();
  var body = message.getPlainBody();

  var ota = null;
  var otaKeys = Object.keys(OTA_SENDERS);
  for (var i = 0; i < otaKeys.length; i++) {
    if (from.indexOf(OTA_SENDERS[otaKeys[i]]) !== -1) {
      ota = otaKeys[i];
      break;
    }
  }
  if (!ota) return null;

  var otaCode = {jalan:'J',rakuten:'R',skyticket:'S',airtrip:'O',airtrip_dp:'O',official:'HP'}[ota] || ota;

  // Check for cancellation
  var isCancellation = CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; });

  if (isCancellation) {
    // キャンセル: DB存在チェック（札幌の予約はDBにあっても那覇GASでは処理しない）
    var tmpId = (ota === 'rakuten') ? extractField_(body, '・予約番号') : extractField_(body, '予約番号');
    if (tmpId && !reservationExists_(tmpId)) {
      Logger.log('Skipping cancel (not in NHA DB): ' + tmpId);
      return {type:'skip', id:tmpId, reason:'DB未登録(札幌)'};
    }
    // 既にキャンセル済みならスキップ（二重CXL防止）
    var existingCxl = tmpId ? reservationExists_(tmpId) : null;
    if (existingCxl && existingCxl.status === 'cancelled') {
      Logger.log('Already cancelled: ' + tmpId);
      return {type:'skip', id:tmpId, reason:'既にキャンセル済み'};
    }
    var cancelId = handleCancellation_(ota, body, dryRun);
    return cancelId ? {type:'cancel', id:cancelId, ota:otaCode} : null;
  }

  // Check subject matches reservation notification
  if (subject.indexOf(OTA_RESERVE_SUBJECTS[ota]) === -1) {
    Logger.log('Skipping non-reservation email (' + ota + '): ' + subject);
    return null;
  }

  // Parse reservation
  var reservation = null;
  switch (ota) {
    case 'jalan':      reservation = parseJalan_(body); break;
    case 'rakuten':    reservation = parseRakuten_(body); break;
    case 'skyticket':  reservation = parseSkyticket_(body); break;
    case 'airtrip':    reservation = parseAirtrip_(body); break;
    case 'airtrip_dp': reservation = parseAirtrip_(body); break;
    case 'official':   reservation = parseOfficial_(body); break;
  }

  if (!reservation) {
    Logger.log('Failed to parse reservation from ' + ota);
    return {type:'failure', id:'不明', ota:otaCode, name:'', reason:'パース失敗'};
  }

  // Filter: 那覇 only
  if (!isNahaReservation_(reservation)) {
    Logger.log('Skipping non-Naha: ' + reservation.id +
      ' (store=' + (reservation._store || '') + ', rawClass=' + (reservation._rawClass || '') + ')');
    return {type:'skip', id:reservation.id, reason:'札幌店'};
  }

  Logger.log('Parsed: ' + reservation.id + ' (' + reservation.ota + ') ' +
    reservation.lend_date + '~' + reservation.return_date + ' class=' + reservation.vehicle);

  if (dryRun) {
    Logger.log('[DRY RUN] Would insert: ' + JSON.stringify(reservation));
    return null;
  }

  // Duplicate check（キャンセル済み同一IDの取り直し対応）
  var existing = reservationExists_(reservation.id);
  if (existing) {
    if (existing.status === 'cancelled') {
      // キャンセル済み → 再有効化（同一IDで取り直し）
      Logger.log('Reactivating cancelled reservation: ' + reservation.id);
      deleteFromFleet_(reservation.id);
      deleteFromTasks_(reservation.id);
      if (!reactivateReservation_(reservation.id, reservation)) {
        return {type:'failure', id:reservation.id, ota:otaCode, name:reservation.name, reason:'再有効化失敗'};
      }
    } else {
      Logger.log('Reservation already exists (active): ' + reservation.id);
      return {type:'skip', id:reservation.id, reason:'登録済み'};
    }
  } else {
    // Insert
    var insertResult = insertReservation_(reservation);
    if (!insertResult) {
      return {type:'failure', id:reservation.id, ota:otaCode, name:reservation.name, reason:'DB登録失敗'};
    }
  }

  // Auto-assign vehicle
  var assigned = autoAssignVehicle_(reservation);
  if (assigned && assigned._preferredModelUnavailable) {
    // 指定車種が空いていない → 未配車（別車種にフォールバックしない）
    return {type:'failure', id:reservation.id, ota:otaCode, name:reservation.name,
      reason:'⚠ 指定車種「' + assigned.preferredModel + '」空車なし（' + reservation.vehicle + 'クラス他車種あり・手動配車必要）',
      dates:reservation.lend_date+'~'+reservation.return_date};
  } else if (assigned) {
    return {type:'success', id:reservation.id, ota:otaCode, name:reservation.name,
      dates:reservation.lend_date+'~'+reservation.return_date,
      vehicle:reservation.vehicle, assignedTo:assigned.name+' ('+assigned.plate_no+')'};
  } else {
    return {type:'failure', id:reservation.id, ota:otaCode, name:reservation.name,
      reason:'配車不可（'+reservation.vehicle+'クラス空車なし）',
      dates:reservation.lend_date+'~'+reservation.return_date};
  }
}

// ============================================================
// Store / Class Filter
// ============================================================
function isNahaReservation_(res) {
  var store = res._store || '';
  var rawClass = res._rawClass || '';
  var address = res._address || '';
  var delPlace = res.del_place || '';
  var colPlace = res.col_place || '';

  // 住所判定: 沖縄 → true, 北海道 → false
  if (/沖縄県|那覇市|沖縄/.test(address)) return true;
  if (/北海道|札幌市/.test(address)) return false;

  // 営業所名判定: 那覇 → true, 札幌 → false
  if (store.indexOf('那覇') !== -1 || store.indexOf('沖縄') !== -1) return true;
  if (store.indexOf('札幌') !== -1) return false;

  // お届け/回収場所判定（HP予約で_storeが空の場合に有効）
  if (/那覇|沖縄|豊見城|宜野湾|浦添|北谷/.test(delPlace + colPlace)) return true;
  if (/札幌|千歳|北海道/.test(delPlace + colPlace)) return false;

  // クラスコード判定: OKA/OKI → true, SPK → false
  if (/_OKA/i.test(rawClass) || /_OKI/i.test(rawClass)) return true;
  if (/_SPK/i.test(rawClass)) return false;

  // 那覇専用クラス（D, A2, B2）なら那覇確定
  if (res.vehicle === 'D' || res.vehicle === 'A2' || res.vehicle === 'B2') return true;

  // ★ 判定不能 → 那覇として取り込む（札幌GASが除外ロジックを持つため、両方で漏れるリスクを回避）
  // 札幌GASのisSapporoReservation_でも判定不能→falseなので、どちらにも入らない問題を防ぐ
  Logger.log('WARNING: Store undetermined, defaulting to NAHA: ' + (res.id || '?') +
    ' vehicle=' + (res.vehicle || '') + ' store=' + store + ' address=' + address +
    ' places=' + delPlace + colPlace + ' rawClass=' + rawClass);
  return true;
}

function extractVehicleClass_(rawClass) {
  if (!rawClass) return '';
  // A2/B2を先にチェック
  if (/A2/i.test(rawClass)) return 'A2';
  if (/B2/i.test(rawClass)) return 'B2';

  // クラス名 + 車種名 マッピング
  var officialMap = {
    'アルファードH': 'A', 'アルファード': 'A',
    'ワンボックスB': 'B', 'ヴェルファイア': 'B', 'セレナ': 'B', 'ヴォクシー': 'B', 'ノア': 'B',
    'コンパクトSUV': 'C', 'ヤリスクロス': 'C', 'ライズ': 'C',
    'ワンボックスD': 'D', 'エスクァイア': 'D',
    'コンパクト': 'F', 'ヴィッツ': 'F', 'ノート': 'F', 'アクア': 'F',
    'ハイブリッド': 'H', 'プリウスアルファ': 'H', 'プリウスα': 'H', 'プリウス': 'H',
    'ハリアー': 'S'
  };
  var omKeys = Object.keys(officialMap).sort(function(a,b){return b.length-a.length;});
  for (var i = 0; i < omKeys.length; i++) {
    if (rawClass.indexOf(omKeys[i]) !== -1) return officialMap[omKeys[i]];
  }

  // _F★ や _F_ や _F(末尾) パターン — ★等の記号も許容
  var m = rawClass.match(/[_]([ABCDSFH])(?:[_★☆\s\)]|$)/i);
  if (m) return m[1].toUpperCase();
  // 先頭パターン: A_xxx
  var m2 = rawClass.match(/^([ABCDSFH])[_]/i);
  if (m2) return m2[1].toUpperCase();
  // スペース後: xxx F_xxx
  var m3 = rawClass.match(/\s([ABCDSFH])[_]/i);
  if (m3) return m3[1].toUpperCase();
  // 末尾: xxx_F
  var m4 = rawClass.match(/[_]([ABCDSFH])$/i);
  if (m4) return m4[1].toUpperCase();
  // 「Xクラス」パターン
  var m5 = rawClass.match(/([ABCDSFH])クラス/i);
  if (m5) return m5[1].toUpperCase();
  return '';
}

// ============================================================
// Field Extraction Helpers
// ============================================================
function extractField_(body, label) {
  var escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  var patterns = [
    new RegExp(escaped + '[：:]\\s*(.+)', 'm'),
    new RegExp(escaped + '\\s+(.+)', 'm')
  ];
  for (var i = 0; i < patterns.length; i++) {
    var m = body.match(patterns[i]);
    if (m) { var val = m[1].trim(); val = val.replace(/^[：:]+\s*/, ''); return val; }
  }
  return '';
}

function parseDateTime_(str) {
  if (!str) return { date: '', time: '' };
  // 2026年4月22日 15:00 or 2026年4月22日 15時00分
  var m = str.match(/(\d{4})年(\d{1,2})月(\d{1,2})日.*?(\d{1,2})[時:](\d{2})/);
  if (m) {
    return {
      date: m[1] + '-' + padZero_(m[2]) + '-' + padZero_(m[3]),
      time: padZero_(m[4]) + ':' + m[5]
    };
  }
  // 2026-04-22 15:00
  m = str.match(/(\d{4})-(\d{1,2})-(\d{1,2}).*?(\d{1,2}):(\d{2})/);
  if (m) {
    return {
      date: m[1] + '-' + padZero_(m[2]) + '-' + padZero_(m[3]),
      time: padZero_(m[4]) + ':' + m[5]
    };
  }
  // 2026/06/20 (土) 09:55 — エアトリプラスDP形式
  m = str.match(/(\d{4})\/(\d{1,2})\/(\d{1,2}).*?(\d{1,2}):(\d{2})/);
  if (m) {
    return {
      date: m[1] + '-' + padZero_(m[2]) + '-' + padZero_(m[3]),
      time: padZero_(m[4]) + ':' + m[5]
    };
  }
  return { date: '', time: '' };
}

function padZero_(n) { return ('0' + parseInt(n, 10)).slice(-2); }
function parsePrice_(str) { if (!str) return 0; return parseInt(str.replace(/[,，円\s]/g, ''), 10) || 0; }
function cleanPhone_(str) { if (!str) return ''; return str.replace(/[^\d-]/g, '').trim(); }
function cleanName_(str) { if (!str) return ''; return str.replace(/\s*様\s*$/, '').trim(); }

// ============================================================
// Parsers
// ============================================================
function parseJalan_(body) {
  var id = extractField_(body, '予約番号');
  if (!id) return null;
  var name = cleanName_(extractField_(body, '予約者氏名'));
  var nameKana = cleanName_(extractField_(body, '運転者氏名カナ'));
  var tel = cleanPhone_(extractField_(body, '運転者電話番号'));
  var mail = extractField_(body, '予約者メールアドレス');
  var lend = parseDateTime_(extractField_(body, '貸出日時'));
  var ret  = parseDateTime_(extractField_(body, '返却日時'));
  var store = extractField_(body, '貸出営業所');
  var rawClass = extractField_(body, '車両クラス');
  var vehicleClass = extractVehicleClass_(rawClass);
  if (!vehicleClass) {
    var plan = extractField_(body, '料金プラン');
    vehicleClass = extractVehicleClass_(plan);
    if (!rawClass) rawClass = plan;
  }
  var insuranceStr = extractField_(body, '補償（任意加入）');
  var insurance = insuranceStr.indexOf('免責') !== -1 ? '免責' : 'なし';
  var peopleStr = extractField_(body, '乗車人数');
  var people = 0;
  var pM = peopleStr.match(/大人\s*(\d+)/);
  if (pM) people += parseInt(pM[1], 10);
  var cM = peopleStr.match(/子供.*?(\d+)/);
  if (cM) people += parseInt(cM[1], 10);
  var price = parsePrice_(extractField_(body, '合計金額'));
  var arrFlight = extractField_(body, '到着便');
  var depFlight = extractField_(body, '出発便');
  var flight = [arrFlight, depFlight].filter(Boolean).join(' / ');
  return {
    id: id, ota: 'J', name: nameKana || name,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: price, status: '確定', tel: tel, mail: mail,
    flight: flight, visit_type: '', del_place: '', col_place: '',
    _store: store, _rawClass: rawClass
  };
}

function parseRakuten_(body) {
  var id = extractField_(body, '・予約番号');
  if (!id) return null;
  var nameKana = cleanName_(extractField_(body, '・予約者氏名（カナ）'));
  var lend = parseDateTime_(extractField_(body, '□貸出日時'));
  var ret  = parseDateTime_(extractField_(body, '□返却日時'));
  var store = extractField_(body, '・貸渡営業所名');
  var detailClass = extractField_(body, '・詳細車両クラス');
  var rawClass = detailClass;
  var vehicleClass = extractVehicleClass_(detailClass);
  if (!vehicleClass) {
    var planMatch = detailClass.match(/プラン[_]([ABCDSFH])/i);
    if (planMatch) {
      vehicleClass = planMatch[1].toUpperCase();
      rawClass = planMatch[1] + '_OKA';
    }
  }
  var optionsStr = extractField_(body, '・オプション/車両の特徴');
  var insurance = optionsStr.indexOf('免責') !== -1 ? '免責' : 'なし';
  var price = parsePrice_(extractField_(body, '（合計）'));
  var optB = 0, optC = 0, optJ = 0;
  var bMatch = optionsStr.match(/ベビーシート\s*(\d*)/);
  if (bMatch) optB = parseInt(bMatch[1], 10) || 1;
  var cMatch = optionsStr.match(/チャイルドシート\s*(\d*)/);
  if (cMatch) optC = parseInt(cMatch[1], 10) || 1;
  var jMatch = optionsStr.match(/ジュニアシート\s*(\d*)/);
  if (jMatch) optJ = parseInt(jMatch[1], 10) || 1;
  return {
    id: id, ota: 'R', name: nameKana,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: 0, insurance: insurance,
    price: price, status: '確定', tel: '', mail: '',
    flight: '', visit_type: '', del_place: '', col_place: '',
    opt_b: optB, opt_c: optC, opt_j: optJ,
    _store: store, _rawClass: rawClass
  };
}

function parseSkyticket_(body) {
  var id = extractField_(body, '予約番号');
  if (!id) return null;
  var nameKana = cleanName_(extractField_(body, 'ご利用者名'));
  var tel = cleanPhone_(extractField_(body, '電話番号'));
  var mail = extractField_(body, 'メールアドレス');
  var lend = parseDateTime_(extractField_(body, '受取日時'));
  var ret  = parseDateTime_(extractField_(body, '返却日時'));
  var store = extractField_(body, '受取店舗');
  var rawClass = extractField_(body, '車両タイプ / クラス');
  if (!rawClass) rawClass = extractField_(body, 'プラン名');
  var vehicleClass = extractVehicleClass_(rawClass);
  var peopleStr = extractField_(body, 'ご利用人数');
  var people = 0;
  var pM = peopleStr.match(/大人\s*(\d+)/);
  if (pM) people += parseInt(pM[1], 10);
  var totalPrice = parsePrice_(extractField_(body, '合計料金'));
  var insurancePriceStr = extractField_(body, '免責補償料金');
  var insurancePrice = parsePrice_(insurancePriceStr);
  var insurance = insurancePrice > 0 ? '免責' : 'なし';
  return {
    id: id, ota: 'S', name: nameKana,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: totalPrice, status: '確定', tel: tel, mail: mail,
    flight: '', visit_type: '', del_place: '', col_place: '',
    _store: store, _rawClass: rawClass
  };
}

function parseAirtrip_(body) {
  var id = extractField_(body, '予約番号');
  if (!id) return null;
  var nameKana = cleanName_(extractField_(body, '予約者名'));
  var tel = cleanPhone_(extractField_(body, '電話番号'));
  var mail = extractField_(body, 'メールアドレス');
  var lend = parseDateTime_(extractField_(body, '貸出日時'));
  var ret  = parseDateTime_(extractField_(body, '返却日時'));
  var store = extractField_(body, '出発営業所');
  var rawClass = extractField_(body, '詳細車両クラス');
  if (!rawClass) rawClass = extractField_(body, 'プラン名');
  var vehicleClass = extractVehicleClass_(rawClass);
  var price = parsePrice_(extractField_(body, '合計金額'));
  var insuranceStr = extractField_(body, '補償オプション');
  var insurance = (insuranceStr && insuranceStr.indexOf('免責') !== -1) ? '免責' : 'なし';
  var arrFlight = extractField_(body, '到着便');
  var depFlight = extractField_(body, '出発便');
  var flight = [arrFlight, depFlight].filter(Boolean).join(' / ');
  return {
    id: id, ota: 'O', name: nameKana,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: 0, insurance: insurance,
    price: price, status: '確定', tel: tel, mail: mail,
    flight: flight, visit_type: '', del_place: '', col_place: '',
    _store: store, _rawClass: rawClass
  };
}

function parseOfficial_(body) {
  var idMatch = body.match(/【予約番号】\s*\n\s*(\S+)/);
  if (!idMatch) return null;
  var id = idMatch[1].trim();
  var nameMatch = body.match(/^(.+?)様/m);
  var name = nameMatch ? nameMatch[1].trim() : '';
  var lendMatch = body.match(/ご利用開始日時\s*\n\s*(\d{4}\/\d{1,2}\/\d{1,2})\s+(\d{1,2}:\d{2})/);
  var lend = { date: '', time: '' };
  if (lendMatch) { lend.date = lendMatch[1].replace(/\//g, '-'); lend.time = lendMatch[2]; }
  var retMatch = body.match(/ご利用終了日時\s*\n\s*(\d{4}\/\d{1,2}\/\d{1,2})\s+(\d{1,2}:\d{2})/);
  var ret = { date: '', time: '' };
  if (retMatch) { ret.date = retMatch[1].replace(/\//g, '-'); ret.time = retMatch[2]; }
  var people = 0;
  var adultMatch = body.match(/大人:\s*(\d+)/);
  if (adultMatch) people += parseInt(adultMatch[1], 10);
  var childMatch = body.match(/子ども:\s*(\d+)/);
  if (childMatch) people += parseInt(childMatch[1], 10);
  // オフィシャル予約: 車両クラス名→クラスコードマッピング
  // 例: 「アルファードHクラス(TOYOTA)」→ A （「H」はHybridではなく車種名の一部）
  var officialClassMap = {
    // クラス名パターン
    'アルファードHクラス(A2)': 'A2', 'アルファードH(A2)': 'A2',
    'アルファードHクラス': 'A', 'アルファードH': 'A',
    'ワンボックスB2': 'B2', 'ワンボックスB': 'B',
    'コンパクトSUV': 'C', 'ワンボックスD': 'D',
    'コンパクト': 'F', 'ハイブリッド': 'H', 'ハリアー': 'S',
    // 車種名パターン（HPが車種名だけで来る場合）
    'アルファード': 'A',
    'ヴェルファイア': 'B', 'セレナ': 'B', 'ヴォクシー': 'B', 'ノア': 'B',
    'ヤリスクロス': 'C', 'ライズ': 'C',
    'エスクァイア': 'D',
    'ヴィッツ': 'F', 'ノート': 'F', 'アクア': 'F',
    'プリウスアルファ': 'H', 'プリウスα': 'H', 'プリウス': 'H',
    'ハリアー': 'S'
  };
  var vehicleClass = '';
  var classLineMatch = body.match(/ご予約車両クラス\s*\n\s*(.+)/);
  if (classLineMatch) {
    var classLine = classLineMatch[1].trim();
    // マッピングテーブルで照合（長い名前を先にチェック）
    var mapKeys = Object.keys(officialClassMap).sort(function(a,b){return b.length - a.length;});
    for (var ci = 0; ci < mapKeys.length; ci++) {
      if (classLine.indexOf(mapKeys[ci]) !== -1) {
        vehicleClass = officialClassMap[mapKeys[ci]];
        break;
      }
    }
    // マッピングで見つからなければ従来のregex（先頭の単独文字）
    if (!vehicleClass) {
      var simpleMatch = classLine.match(/^(A2|B2|[ABCDSFH])クラス/i);
      if (simpleMatch) vehicleClass = simpleMatch[1].toUpperCase();
    }
  }
  // さらにフォールバック
  if (!vehicleClass) {
    var planMatch = body.match(/([ABCDSFH]2?)クラス/i);
    if (planMatch) vehicleClass = planMatch[1].toUpperCase();
  }
  var insurance = 'なし';
  if (/免責補償制度\(CDW\):\s*あり/.test(body)) insurance = '免責';
  if (/レンタカー安心パック:\s*あり/.test(body)) insurance = 'NOC';
  var optB = 0, optC = 0, optJ = 0;
  var cbMatch = body.match(/チャイルドシート\(チャイルド\):\s*(\d+)\s*台/);
  if (cbMatch) optC = parseInt(cbMatch[1], 10);
  if (!cbMatch) { var cbAlt = body.match(/チャイルドシート\(チャイルド\):\s*あり\s*(\d*)/); if (cbAlt) optC = parseInt(cbAlt[1], 10) || 1; }
  var jbMatch = body.match(/チャイルドシート\(ジュニア\):\s*(\d+)\s*台/);
  if (jbMatch) optJ = parseInt(jbMatch[1], 10);
  if (!jbMatch) { var jbAlt = body.match(/チャイルドシート\(ジュニア\):\s*あり\s*(\d*)/); if (jbAlt) optJ = parseInt(jbAlt[1], 10) || 1; }
  var priceMatch = body.match(/料金\s*\n\s*(\d[\d,]*)\s*円/);
  var price = priceMatch ? parsePrice_(priceMatch[1]) : 0;
  var telMatch = body.match(/【電話番号】\s*\n\s*(\S+)/);
  var tel = telMatch ? cleanPhone_(telMatch[1]) : '';
  var mailMatch = body.match(/【メールアドレス】\s*\n\s*(\S+)/);
  var mail = mailMatch ? mailMatch[1].trim() : '';
  var delPlaceMatch = body.match(/【お届け場所名】\s*\n\s*(.+)/);
  var delPlace = delPlaceMatch ? delPlaceMatch[1].trim() : '';
  var colPlaceMatch = body.match(/【回収場所名】\s*\n\s*(.+)/);
  var colPlace = colPlaceMatch ? colPlaceMatch[1].trim() : '';
  var addressMatch = body.match(/【お届け場所住所】\s*\n\s*(.+)/);
  var address = addressMatch ? addressMatch[1].trim() : '';
  return {
    id: id, ota: 'HP', name: name,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: price, status: '確定', tel: tel, mail: mail,
    flight: '', visit_type: '', del_place: delPlace, col_place: colPlace,
    opt_b: optB, opt_c: optC, opt_j: optJ,
    _store: '', _rawClass: vehicleClass, _address: address,
    _vehicleModel: classLineMatch ? classLineMatch[1].replace(/\(.*?\)/g,'').replace(/[_](ハイブリッド|HYBRID|hybrid|ガソリン|ディーゼル)$/,'').trim() : ''
  };
}

// ============================================================
// Cancellation Handler
// ============================================================
function handleCancellation_(ota, body, dryRun) {
  var reservationId = '';
  if (ota === 'rakuten') {
    reservationId = extractField_(body, '・予約番号');
  } else {
    reservationId = extractField_(body, '予約番号');
  }

  if (!reservationId) {
    Logger.log('Cancellation: could not extract reservation ID (' + ota + ')');
    return;
  }

  Logger.log('Cancellation detected: ' + reservationId + ' (' + ota + ')');

  if (dryRun) {
    Logger.log('[DRY RUN] Would cancel: ' + reservationId);
    return;
  }

  // Delete fleet + tasks, update reservation status to キャンセル
  deleteFromFleet_(reservationId);
  deleteFromTasks_(reservationId);
  supabaseUpdate_('nha_reservations', 'id=eq.' + encodeURIComponent(reservationId), {status: 'cancelled'});

  Logger.log('Cancelled reservation: ' + reservationId);
  return reservationId;
}

// ============================================================
// Supabase API
// ============================================================
function supabaseHeaders_() {
  var key = getSupabaseKey_();
  return {
    'apikey': key,
    'Authorization': 'Bearer ' + key,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
}

function supabaseGet_(table, queryParams) {
  // ★ Supabase REST APIはデフォルト1000件制限。必要に応じてlimitを明示的に付与
  var sep = queryParams ? '&' : '';
  if (queryParams.indexOf('limit=') === -1) {
    queryParams += sep + 'limit=5000';
  }
  var url = getSupabaseUrl_() + '/rest/v1/' + table + '?' + queryParams;
  var resp = UrlFetchApp.fetch(url, {
    method: 'GET',
    headers: supabaseHeaders_(),
    muteHttpExceptions: true
  });
  if (resp.getResponseCode() >= 400) {
    Logger.log('Supabase GET error (' + table + '): ' + resp.getContentText());
    return [];
  }
  return JSON.parse(resp.getContentText());
}

function supabasePost_(table, data) {
  var url = getSupabaseUrl_() + '/rest/v1/' + table;
  var resp = UrlFetchApp.fetch(url, {
    method: 'POST',
    headers: supabaseHeaders_(),
    payload: JSON.stringify(data),
    muteHttpExceptions: true
  });
  if (resp.getResponseCode() >= 400) {
    Logger.log('Supabase POST error (' + table + '): ' + resp.getContentText());
    return null;
  }
  return JSON.parse(resp.getContentText());
}

function supabaseUpdate_(table, queryParams, data) {
  var url = getSupabaseUrl_() + '/rest/v1/' + table + '?' + queryParams;
  var resp = UrlFetchApp.fetch(url, {
    method: 'PATCH',
    headers: supabaseHeaders_(),
    payload: JSON.stringify(data),
    muteHttpExceptions: true
  });
  return resp.getResponseCode() < 400;
}

function supabaseDelete_(table, queryParams) {
  var url = getSupabaseUrl_() + '/rest/v1/' + table + '?' + queryParams;
  var resp = UrlFetchApp.fetch(url, {
    method: 'DELETE',
    headers: supabaseHeaders_(),
    muteHttpExceptions: true
  });
  if (resp.getResponseCode() >= 400) {
    Logger.log('Supabase DELETE error (' + table + '): ' + resp.getContentText());
    return false;
  }
  return true;
}

// ============================================================
// Reservation DB Operations
// ============================================================
function reservationExists_(reservationId) {
  var rows = supabaseGet_('nha_reservations', 'id=eq.' + encodeURIComponent(reservationId) + '&select=id,status');
  return rows.length > 0 ? rows[0] : null;
}

// GAS内部フィールド → nha_reservationsカラム名変換
function toDbRow_(reservation) {
  var row = {
    id: reservation.id,
    name: reservation.name || '',
    ota: reservation.ota || '',
    start_date: reservation.lend_date || '',
    end_date: reservation.return_date || '',
    start_time: reservation.lend_time || '',
    end_time: reservation.return_time || '',
    col_time: reservation.return_time || '',
    del_time: reservation.lend_time || '',
    vehicle_class: reservation.vehicle || '',
    people: reservation.people || 0,
    insurance: reservation.insurance || '',
    amount: reservation.price || 0,
    price: reservation.price || 0,
    tel: reservation.tel || '',
    mail: reservation.mail || '',
    del_flight: reservation.flight || '',
    del_place: reservation.del_place || '',
    col_place: reservation.col_place || '',
    car_seat: String(reservation.opt_c || 0),
    junior_seat: String(reservation.opt_j || 0),
    visit_type: reservation.visit_type || '',
    status: 'confirmed'
  };
  return row;
}

// キャンセル済み予約を再有効化（同一IDで取り直しされた場合）
function reactivateReservation_(reservationId, reservation) {
  var row = toDbRow_(reservation);
  row.status = 'confirmed';
  var ok = supabaseUpdate_('nha_reservations', 'id=eq.' + encodeURIComponent(reservationId), row);
  if (ok) Logger.log('Reactivated cancelled reservation: ' + reservationId);
  return ok;
}

function insertReservation_(reservation) {
  var row = toDbRow_(reservation);
  var result = supabasePost_('nha_reservations', row);
  if (result) Logger.log('Inserted reservation: ' + reservation.id);
  return result;
}

function deleteReservation_(reservationId) {
  return supabaseDelete_('nha_reservations', 'id=eq.' + encodeURIComponent(reservationId));
}

function deleteFromFleet_(reservationId) {
  return supabaseDelete_('nha_fleet', 'reservation_id=eq.' + encodeURIComponent(reservationId));
}

function deleteFromTasks_(reservationId) {
  return supabaseDelete_('nha_tasks', 'reservation_id=eq.' + encodeURIComponent(reservationId));
}

// ============================================================
// Vehicle Auto-Assignment
// ============================================================
function autoAssignVehicle_(reservation) {
  var vehicleClass = reservation.vehicle;
  if (!vehicleClass) {
    Logger.log('No vehicle class for ' + reservation.id + '. Will be 未配車.');
    return;
  }

  // A2→A, B2→Bフォールバック（同じ車種構成のため）
  var searchClass = vehicleClass;
  if (vehicleClass === 'A2') searchClass = 'A';
  if (vehicleClass === 'B2') searchClass = 'B';

  var vehicles = supabaseGet_('nha_vehicles',
    'type=eq.' + encodeURIComponent(searchClass) + '&insurance_veh=eq.false&select=code,name,plate_no,seats');
  if (vehicles.length === 0) {
    Logger.log('No vehicles of class ' + searchClass + ' (original: ' + vehicleClass + '). ' + reservation.id + ' will be 未配車.');
    return;
  }

  var lendDate = reservation.lend_date;
  var returnDate = reservation.return_date;

  var busyVehicleCodes = {};
  var overlappingFleet = getOverlappingFleetVehicles_(lendDate, returnDate);
  for (var i = 0; i < overlappingFleet.length; i++) {
    busyVehicleCodes[overlappingFleet[i]] = true;
  }

  var overlappingMaint = getOverlappingMaintenance_(lendDate, returnDate);
  for (var i = 0; i < overlappingMaint.length; i++) {
    busyVehicleCodes[overlappingMaint[i].vehicle_code] = true;
  }

  // 車種名指定がある場合、指定車種のみ検索（フォールバックしない）
  var preferredModel = reservation._vehicleModel || '';
  var assignedVehicle = null;
  if (preferredModel) {
    for (var i = 0; i < vehicles.length; i++) {
      var v = vehicles[i];
      if (busyVehicleCodes[v.code]) continue;
      if (v.name.indexOf(preferredModel) !== -1) {
        assignedVehicle = v;
        break;
      }
    }
    if (assignedVehicle) {
      Logger.log('Preferred model match: ' + preferredModel + ' → ' + assignedVehicle.code);
    } else {
      // 指定車種が空いていない → 未配車（別車種にフォールバックしない）
      Logger.log('⚠ Preferred model "' + preferredModel + '" not available for ' + reservation.id +
        ' (' + lendDate + '~' + returnDate + '). Will be 未配車（手動対応）.');
      return { _preferredModelUnavailable: true, preferredModel: preferredModel };
    }
  }
  // 車種指定なしの場合のみクラス内の先頭空車
  if (!assignedVehicle) {
    for (var i = 0; i < vehicles.length; i++) {
      var v = vehicles[i];
      if (busyVehicleCodes[v.code]) continue;
      assignedVehicle = v;
      break;
    }
  }

  if (!assignedVehicle) {
    Logger.log('No available vehicle for class ' + vehicleClass +
      ' (' + lendDate + '~' + returnDate + '). ' + reservation.id + ' will be 未配車.');
    return null;
  }

  var fleetRow = { reservation_id: reservation.id, vehicle_code: assignedVehicle.code };
  var result = supabasePost_('nha_fleet', fleetRow);
  if (result) {
    Logger.log('Assigned ' + assignedVehicle.code + ' (' + assignedVehicle.name + ') to ' + reservation.id);
    return assignedVehicle;
  }
  return null;
}

function getOverlappingFleetVehicles_(lendDate, returnDate) {
  // ★ DB側で期間重複を絞り込む（全件取得を避ける）
  // nha_reservations.start_date <= returnDate AND nha_reservations.end_date >= lendDate
  var query = 'select=vehicle_code,reservation_id,nha_reservations!inner(start_date,end_date)' +
    '&nha_reservations.start_date=lte.' + encodeURIComponent(returnDate) +
    '&nha_reservations.end_date=gte.' + encodeURIComponent(lendDate);
  var overlapping = supabaseGet_('nha_fleet', query);
  var busyCodes = [];
  for (var i = 0; i < overlapping.length; i++) {
    busyCodes.push(overlapping[i].vehicle_code);
  }
  return busyCodes;
}

function getOverlappingMaintenance_(lendDate, returnDate) {
  var query = 'start_date=lte.' + encodeURIComponent(returnDate) +
    '&end_date=gte.' + encodeURIComponent(lendDate) +
    '&select=vehicle_code';
  return supabaseGet_('nha_maintenance', query);
}

// ============================================================
// Slack Notifications
// ============================================================
function sendSlackSuccess_(items) {
  var lines = ['✅ 那覇店新規予約取込完了通知', ''];
  items.forEach(function(r) {
    lines.push('【' + r.ota + '】' + r.id);
    lines.push('  ' + r.name + ' / ' + r.dates + ' / ' + r.vehicle + 'クラス');
    lines.push('  → 配車: ' + r.assignedTo);
    lines.push('');
  });
  lines.push('合計: ' + items.length + '件');
  MailApp.sendEmail(getSlackEmail_(), '✅ 那覇店新規予約取込完了通知 ' + items.length + '件', lines.join('\n'));
  Logger.log('Slack success notification sent: ' + items.length + '件');
}

function sendSlackFailure_(items) {
  var lines = ['❌ 那覇店新規予約取込失敗通知', ''];
  items.forEach(function(r) {
    lines.push('【' + r.ota + '】' + (r.id || '不明'));
    if (r.name) lines.push('  ' + r.name + (r.dates ? ' / ' + r.dates : ''));
    lines.push('  理由: ' + r.reason);
    lines.push('');
  });
  lines.push('合計: ' + items.length + '件 ※手動対応が必要です');
  MailApp.sendEmail(getSlackEmail_(), '❌ 那覇店新規予約取込失敗通知 ' + items.length + '件', lines.join('\n'));
  Logger.log('Slack failure notification sent: ' + items.length + '件');
}

function sendSlackCancel_(items) {
  var lines = ['🔄 那覇店予約キャンセル処理通知', ''];
  items.forEach(function(r) {
    lines.push('【' + r.ota + '】' + r.id + ' → キャンセル処理完了');
  });
  lines.push('');
  lines.push('合計: ' + items.length + '件');
  MailApp.sendEmail(getSlackEmail_(), '🔄 那覇店予約キャンセル処理 ' + items.length + '件', lines.join('\n'));
  Logger.log('Slack cancel notification sent: ' + items.length + '件');
}

// ============================================================
// Heartbeat & Monitoring
// ============================================================

// ハートビート書込み: 実行のたびにapp_settingsに記録
function updateHeartbeat_(key, stats) {
  try {
    var payload = {
      key: 'heartbeat_' + key,
      value: JSON.stringify({
        last_run: new Date().toISOString(),
        status: (stats.failure || 0) > 0 ? 'warning' : 'ok',
        processed: (stats.success || 0) + (stats.cancel || 0) + (stats.skip || 0),
        errors: stats.failure || 0,
        details: stats
      })
    };
    var hbKey = getSupabaseKey_();
    var options = {
      method: 'post',
      headers: {
        'apikey': hbKey,
        'Authorization': 'Bearer ' + hbKey,
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    };
    UrlFetchApp.fetch(getSupabaseUrl_() + '/rest/v1/nha_app_settings', options);
    Logger.log('[Heartbeat] Updated: ' + key);
  } catch (e) {
    Logger.log('[Heartbeat] Error: ' + e.message);
  }
}

// 監視チェック: 30分間隔で実行。ハートビートが途絶えていたらSlack通知
function checkHeartbeats() {
  var checks = [
    { key: 'nha_gas_email', label: '那覇GAS予約取込', thresholdMin: 30 }
  ];

  checks.forEach(function(check) {
    try {
      var chkKey = getSupabaseKey_();
      var url = getSupabaseUrl_() + '/rest/v1/app_settings?key=eq.heartbeat_' + check.key + '&select=value';
      var options = {
        method: 'get',
        headers: {
          'apikey': chkKey,
          'Authorization': 'Bearer ' + chkKey
        },
        muteHttpExceptions: true
      };
      var res = UrlFetchApp.fetch(url, options);
      var data = JSON.parse(res.getContentText());
      var props = PropertiesService.getScriptProperties();

      if (!data || data.length === 0) {
        var initKey = 'alert_init_' + check.key;
        if (!props.getProperty(initKey)) {
          sendSlackAlert_('⚠️ ' + check.label + ': ハートビート未登録（初回実行待ち）');
          props.setProperty(initKey, 'true');
        }
        return;
      }

      var hb = JSON.parse(data[0].value);
      var lastRun = new Date(hb.last_run);
      var now = new Date();
      var diffMin = Math.round((now - lastRun) / 60000);

      // ScriptProperties で通知済みフラグ管理（同じ障害で連続通知しない）
      var props = PropertiesService.getScriptProperties();
      var alertKey = 'alert_sent_' + check.key;
      var alertSent = props.getProperty(alertKey);

      if (diffMin > check.thresholdMin) {
        if (!alertSent) {
          var timeStr = Utilities.formatDate(lastRun, 'Asia/Tokyo', 'MM/dd HH:mm');
          sendSlackAlert_('🚨 ' + check.label + ' が' + diffMin + '分間停止中\n最終実行: ' + timeStr + '\n処理数: ' + (hb.processed || 0) + '件 / エラー: ' + (hb.errors || 0) + '件');
          props.setProperty(alertKey, 'true');
        }
      } else {
        // 復旧検知
        if (alertSent) {
          sendSlackAlert_('✅ ' + check.label + ' 復旧しました（停止' + diffMin + '分）');
          props.deleteProperty(alertKey);
        }
      }
    } catch (e) {
      Logger.log('[checkHeartbeats] Error for ' + check.key + ': ' + e.message);
    }
  });
}

function sendSlackAlert_(message) {
  try {
    MailApp.sendEmail(getSlackEmail_(), message.split('\n')[0], message);
    Logger.log('[Alert] Sent: ' + message.split('\n')[0]);
  } catch (e) {
    Logger.log('[Alert] Send error: ' + e.message);
  }
}

// ============================================================
// 未知送信元監視: OTA_SENDERSに未登録の予約メールを検知
// ============================================================
function checkUnknownSenders_() {
  var knownSenders = Object.values(OTA_SENDERS);
  // 予約系キーワードを含むreserve@宛メールを直近2日で検索
  var reserveKeywords = ['予約確定', '予約通知', '予約受付', '新規予約', 'ご予約完了', '予約を受け付け'];
  var query = 'to:reserve@rent-handyman.jp newer_than:2d -label:' + LABEL_NAME;
  var threads;
  try {
    threads = GmailApp.search(query, 0, 50);
  } catch (e) {
    Logger.log('checkUnknownSenders_ search error: ' + e.message);
    return;
  }
  if (threads.length === 0) return;

  var unknowns = [];
  var checkedKey = 'nha_unknown_senders_alerted';
  var alerted = {};
  try {
    var raw = PropertiesService.getScriptProperties().getProperty(checkedKey);
    if (raw) alerted = JSON.parse(raw);
  } catch (e) {}

  for (var i = 0; i < threads.length; i++) {
    var msgs = threads[i].getMessages();
    for (var j = 0; j < msgs.length; j++) {
      var from = msgs[j].getFrom();
      var subject = msgs[j].getSubject();
      var msgId = msgs[j].getId();

      // 既にアラート済みならスキップ
      if (alerted[msgId]) continue;

      // 既知の送信元ならスキップ
      var isKnown = knownSenders.some(function(s) { return from.indexOf(s) !== -1; });
      if (isKnown) continue;

      // 件名に予約キーワードが含まれるか
      var hasReserveKeyword = reserveKeywords.some(function(kw) { return subject.indexOf(kw) !== -1; });
      if (!hasReserveKeyword) continue;

      // 未知の予約メール発見
      unknowns.push({
        from: from,
        subject: subject,
        date: msgs[j].getDate().toLocaleString('ja-JP'),
        msgId: msgId
      });
      alerted[msgId] = true;
    }
  }

  if (unknowns.length > 0) {
    // Slack警告送信
    var lines = ['⚠️ 那覇店 未知の予約メール検知 ' + unknowns.length + '件', ''];
    for (var u = 0; u < unknowns.length; u++) {
      lines.push('From: ' + unknowns[u].from);
      lines.push('件名: ' + unknowns[u].subject);
      lines.push('日時: ' + unknowns[u].date);
      lines.push('---');
    }
    lines.push('');
    lines.push('※ GASのOTA_SENDERSに未登録の送信元です。');
    lines.push('※ 自動取込されていない可能性があります。要確認。');
    sendSlackAlert_(lines.join('\n'));
    Logger.log('Unknown sender alert sent: ' + unknowns.length + ' email(s)');

    // アラート済みを記録（同じメールで重複通知しない）
    try {
      PropertiesService.getScriptProperties().setProperty(checkedKey, JSON.stringify(alerted));
    } catch (e) {}
  }
}

// セットアップ: 監視トリガー追加（30分間隔）
function setupMonitoring() {
  // 既存の監視トリガーを削除
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'checkHeartbeats') {
      ScriptApp.deleteTrigger(t);
    }
  });

  ScriptApp.newTrigger('checkHeartbeats')
    .timeBased()
    .everyMinutes(30)
    .create();

  Logger.log('Monitoring setup complete: 30-minute heartbeat check trigger created.');
}

// ============================================================
// Gmail Helpers
// ============================================================
// 既存メール全てにprocessed_nahaラベルを付与（初回セットアップ用・1回だけ実行）
function markAllExistingAsProcessed() {
  var label = getOrCreateLabel_(LABEL_NAME);
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') -label:' + LABEL_NAME;
  var threads = GmailApp.search(query, 0, 500);
  Logger.log('Marking ' + threads.length + ' threads as processed_naha');
  for (var i = 0; i < threads.length; i++) {
    threads[i].addLabel(label);
  }
  Logger.log('Done. All existing emails marked as processed_naha.');
}

function getOrCreateLabel_(labelName) {
  var label = GmailApp.getUserLabelByName(labelName);
  if (!label) {
    label = GmailApp.createLabel(labelName);
    Logger.log('Created Gmail label: ' + labelName);
  }
  return label;
}

// ============================================================
// Reprocess: 特定予約IDの再処理（手動実行用）
// ============================================================
/**
 * 指定した予約IDのメールを再検索し、再処理する。
 * - DB未登録 → メールを再パース → insert → 自動配車
 * - DB登録済み＆vehicle_class空 → メールからクラス取得 → DB更新 → 自動配車
 * GASエディタから手動実行する。
 */
function reprocessByIds() {
  var targetIds = ['MTT21315'];
  var label = getOrCreateLabel_(LABEL_NAME);
  var successes = [];
  var failures = [];

  for (var t = 0; t < targetIds.length; t++) {
    var targetId = targetIds[t];
    Logger.log('=== Reprocessing: ' + targetId + ' ===');

    // 1. DB状態チェック
    var existing = reservationExists_(targetId);

    if (existing && existing.status !== 'cancelled') {
      // DB登録済み（LOZ81086のケース）→ vehicle_classが空なら修正
      var fullRes = supabaseGet_('nha_reservations', 'id=eq.' + encodeURIComponent(targetId) + '&select=*');
      if (fullRes.length === 0) {
        failures.push({id: targetId, ota: '?', name: '', reason: 'DB参照失敗'});
        continue;
      }
      var dbRow = fullRes[0];
      if (dbRow.vehicle_class && dbRow.vehicle_class !== '') {
        // 既にクラスがある → 配車だけ確認
        Logger.log(targetId + ' already has vehicle_class=' + dbRow.vehicle_class + '. Checking fleet...');
        var fleetCheck = supabaseGet_('nha_fleet', 'reservation_id=eq.' + encodeURIComponent(targetId) + '&select=vehicle_code');
        if (fleetCheck.length > 0) {
          Logger.log(targetId + ' already assigned to ' + fleetCheck[0].vehicle_code + '. Skipping.');
          continue;
        }
        // 配車なし → 自動配車を試行
        var fakeRes = {
          id: targetId, vehicle: dbRow.vehicle_class,
          lend_date: dbRow.start_date, return_date: dbRow.end_date, name: dbRow.name, ota: dbRow.ota
        };
        var assigned = autoAssignVehicle_(fakeRes);
        if (assigned) {
          successes.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
            dates: dbRow.start_date + '~' + dbRow.end_date,
            vehicle: dbRow.vehicle_class, assignedTo: assigned.name + ' (' + assigned.plate_no + ')'});
        } else {
          failures.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
            reason: '配車不可（' + dbRow.vehicle_class + 'クラス空車なし）',
            dates: dbRow.start_date + '~' + dbRow.end_date});
        }
        continue;
      }

      // vehicle_class が空 → メールから再取得
      Logger.log(targetId + ' has empty vehicle_class. Searching email...');
      var emailData = findEmailByReservationId_(targetId);
      if (!emailData) {
        failures.push({id: targetId, ota: dbRow.ota || '?', name: dbRow.name || '', reason: 'メール検索失敗'});
        continue;
      }
      var parsed = emailData.parsed;
      if (!parsed || !parsed.vehicle) {
        failures.push({id: targetId, ota: dbRow.ota || '?', name: dbRow.name || '', reason: 'クラス抽出失敗'});
        continue;
      }
      // DB更新
      supabaseUpdate_('nha_reservations', 'id=eq.' + encodeURIComponent(targetId),
        {vehicle_class: parsed.vehicle});
      Logger.log('Updated vehicle_class=' + parsed.vehicle + ' for ' + targetId);

      // 自動配車
      var fakeRes2 = {
        id: targetId, vehicle: parsed.vehicle,
        lend_date: dbRow.start_date, return_date: dbRow.end_date, name: dbRow.name, ota: dbRow.ota
      };
      var assigned2 = autoAssignVehicle_(fakeRes2);
      if (assigned2) {
        successes.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
          dates: dbRow.start_date + '~' + dbRow.end_date,
          vehicle: parsed.vehicle, assignedTo: assigned2.name + ' (' + assigned2.plate_no + ')'});
      } else {
        failures.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
          reason: '配車不可（' + parsed.vehicle + 'クラス空車なし）',
          dates: dbRow.start_date + '~' + dbRow.end_date});
      }
      continue;
    }

    // 2. DB未登録またはキャンセル済み（OPX93188, C260301451のケース）→ メール再取得＆処理
    Logger.log(targetId + ' not in DB (or cancelled). Searching email...');
    var emailData2 = findEmailByReservationId_(targetId);
    if (!emailData2) {
      failures.push({id: targetId, ota: '?', name: '', reason: 'メール未発見'});
      continue;
    }

    // processed_nahaラベルを除去（再処理のため）
    try {
      emailData2.thread.removeLabel(label);
      Logger.log('Removed processed_naha label from thread for ' + targetId);
    } catch (e) {
      Logger.log('Label removal warning: ' + e.message);
    }

    // processMessage_で処理
    var result = processMessage_(emailData2.message, false);

    // 処理後ラベルを再付与
    try { emailData2.thread.addLabel(label); } catch (e) {}

    if (result) {
      if (result.type === 'success') successes.push(result);
      else if (result.type === 'failure') failures.push(result);
      else Logger.log(targetId + ' result: ' + result.type + ' - ' + (result.reason || ''));
    } else {
      failures.push({id: targetId, ota: '?', name: '', reason: 'processMessage_がnullを返した'});
    }
  }

  // 結果通知
  Logger.log('=== Reprocess complete ===');
  Logger.log('Success: ' + successes.length + ', Failure: ' + failures.length);
  if (successes.length > 0) sendSlackSuccess_(successes);
  if (failures.length > 0) sendSlackFailure_(failures);
}

/**
 * 予約番号でGmailを検索し、該当メッセージとパース結果を返す
 */
function findEmailByReservationId_(reservationId) {
  // まずOTA送信元フィルター付きで検索
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') ' + reservationId;
  var threads = GmailApp.search(query, 0, 10);

  // 見つからなければ予約番号のみで再検索（from制約を外す）
  if (threads.length === 0) {
    Logger.log('Retry search without from filter: ' + reservationId);
    threads = GmailApp.search('"' + reservationId + '"', 0, 10);
  }

  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();
    for (var j = 0; j < messages.length; j++) {
      var msg = messages[j];
      var body = msg.getPlainBody();
      if (body.indexOf(reservationId) === -1) continue;

      var subject = msg.getSubject();
      // キャンセルメールはスキップ
      if (CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; })) continue;

      // OTA判定
      var from = msg.getFrom();
      var ota = null;
      var otaKeys = Object.keys(OTA_SENDERS);
      for (var k = 0; k < otaKeys.length; k++) {
        if (from.indexOf(OTA_SENDERS[otaKeys[k]]) !== -1) { ota = otaKeys[k]; break; }
      }
      if (!ota) continue;

      // パース
      var parsed = null;
      switch (ota) {
        case 'jalan':      parsed = parseJalan_(body); break;
        case 'rakuten':    parsed = parseRakuten_(body); break;
        case 'skyticket':  parsed = parseSkyticket_(body); break;
        case 'airtrip':    parsed = parseAirtrip_(body); break;
        case 'airtrip_dp': parsed = parseAirtrip_(body); break;
        case 'official':   parsed = parseOfficial_(body); break;
      }

      if (parsed) {
        Logger.log('Found email for ' + reservationId + ': OTA=' + ota + ' class=' + (parsed.vehicle || 'empty'));
        return {message: msg, thread: threads[i], parsed: parsed, ota: ota};
      }
    }
  }

  Logger.log('Email not found for: ' + reservationId);
  return null;
}
