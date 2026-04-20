// ============================================================
// GAS - Reservation Email Import & Vehicle Auto-Assignment
// Gmail: (メールアドレス未定)
// Target: 高松 (TKM) store only — BUDDICA
// OTA: 楽天(R), じゃらん(J), skyticket(S), エアトリ(O), HP, GoGoOut(G), レンタカードットコム(RC)
// ============================================================

// --- Supabase Config (ScriptProperties経由) ---
var LABEL_NAME = 'processed_takamatsu';
function getSupabaseUrl_() { return PropertiesService.getScriptProperties().getProperty('SUPABASE_URL'); }
function getSupabaseKey_() { return PropertiesService.getScriptProperties().getProperty('SUPABASE_KEY'); }
function getSlackEmail_() { return PropertiesService.getScriptProperties().getProperty('SLACK_EMAIL'); }

function setupProperties() {
  PropertiesService.getScriptProperties().setProperties({
    'SUPABASE_URL': 'https://iuxsjjyfzjyohqvnaxei.supabase.co',
    'SUPABASE_KEY': '<SERVICE_ROLE_KEYをSupabase Dashboard > Settings > APIから取得して入力>',
    'SLACK_EMAIL': '<高松店用Slackチャンネルのメールアドレスを入力>'
  });
  Logger.log('Properties set.');
}

// --- OTA sender definitions ---
var OTA_SENDERS = {
  jalan:     'info@jalan-rentacar.jalan.net',
  rakuten:   'travel@mail.travel.rakuten.co.jp',
  skyticket: 'rentacar@skyticket.com',
  airtrip:   'info@rentacar-mail.airtrip.jp',
  airtrip_dp: 'info@skygate.co.jp',
  official:  'noreply@rent-handyman.jp',
  gogoout:   'service@gogoout.com',
  rentacar_dc: 'info@rentacar.com',
  rentacar_dc2: 'info@web-rentacar.com'
};

// --- OTA reservation subject patterns ---
var OTA_RESERVE_SUBJECTS = {
  jalan:     'じゃらんnetレンタカー 予約通知',
  rakuten:   '【楽天トラベル】予約受付のお知らせ',
  skyticket: '【skyticket】 新規予約',
  airtrip:   '【予約確定】エアトリレンタカー',
  airtrip_dp: '【予約確定】エアトリプラス',
  official:  'ご予約完了のお知らせ',
  gogoout:   'gogoout - 予約のお知らせ',
  rentacar_dc: '予約登録のお知らせ',
  rentacar_dc2: '予約登録のお知らせ'
};

// --- Cancellation keywords in subject ---
var CANCEL_KEYWORDS = ['予約キャンセル受付', 'キャンセル', 'cancellation', 'cancelled'];

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
  var SUPABASE_URL = getSupabaseUrl_();
  var SUPABASE_KEY = getSupabaseKey_();
  var SLACK_EMAIL = getSlackEmail_();
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    Logger.log('ERROR: SUPABASE_URL or SUPABASE_KEY not set. Run setupProperties() first.');
    return;
  }

  var label = getOrCreateLabel_(LABEL_NAME);
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  // ラベルでフィルタしない（キャンセルメールが同スレッドに来てもスキップされない）
  // 代わりにメッセージID単位で処理済み管理する
  var query = '(' + fromClause + ') newer_than:2d';

  var threads = GmailApp.search(query, 0, 50);
  if (threads.length === 0) {
    Logger.log('No new reservation emails found.');
    return;
  }

  Logger.log('Found ' + threads.length + ' thread(s) to scan.');

  // メッセージID単位の処理済みセットを取得
  var processedMsgIds = getProcessedMsgIds_();
  var now = Date.now();
  // 3日以上前のエントリを削除（PropertiesServiceサイズ制限対策）
  var THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000;
  var pruneKeys = Object.keys(processedMsgIds);
  for (var p = 0; p < pruneKeys.length; p++) {
    if (now - processedMsgIds[pruneKeys[p]] > THREE_DAYS_MS) {
      delete processedMsgIds[pruneKeys[p]];
    }
  }

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
    var msgId = allMessages[i].msg.getId();

    // メッセージID単位でスキップ（ラベルではなくIDで判定）
    if (processedMsgIds[msgId]) {
      continue;
    }

    try {
      var result = processMessage_(allMessages[i].msg, false);
      if (result) {
        if (result.type === 'success') successes.push(result);
        else if (result.type === 'failure') failures.push(result);
        else if (result.type === 'cancel') cancellations.push(result);
        else if (result.type === 'skip') skipped.push(result);
      }
    } catch (e) {
      Logger.log('ERROR processing message ID ' + msgId + ': ' + e.message + '\n' + e.stack);
      failures.push({id: '不明', ota: '?', name: '', reason: 'エラー: ' + e.message});
    }

    // 処理結果に関わらずメッセージIDを記録（二重処理防止）
    processedMsgIds[msgId] = now;

    // ラベルは視覚目印として付与（機能的ゲートキーパーではない）
    var tid = allMessages[i].thread.getId();
    if (!labeledThreads[tid]) {
      allMessages[i].thread.addLabel(label);
      labeledThreads[tid] = true;
    }
  }

  // 処理済みメッセージIDを保存
  saveProcessedMsgIds_(processedMsgIds);

  if (successes.length > 0) sendSlackSuccess_(successes);
  if (failures.length > 0) sendSlackFailure_(failures);
  if (cancellations.length > 0) sendSlackCancel_(cancellations);

  // ハートビート: 実行完了をDBに記録
  updateHeartbeat_('tkm_gas_email', {
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

  var otaCode = {jalan:'J',rakuten:'R',skyticket:'S',airtrip:'O',airtrip_dp:'O',official:'HP',gogoout:'G',rentacar_dc:'RC',rentacar_dc2:'RC'}[ota] || ota;

  // Check for cancellation
  var isCancellation = CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; });

  if (isCancellation) {
    // キャンセル: DB存在チェック（1回のDB呼出しで判定）
    var tmpId = (ota === 'rakuten') ? extractField_(body, '・予約番号') : extractField_(body, '予約番号');
    if (tmpId) {
      var existing = reservationExists_(tmpId);
      if (!existing) {
        Logger.log('Skipping cancel (not in TKM DB): ' + tmpId);
        return {type:'skip', id:tmpId, reason:'DB未登録(他店)'};
      }
      if (existing.status === 'cancelled') {
        Logger.log('Already cancelled: ' + tmpId);
        return {type:'skip', id:tmpId, reason:'既にキャンセル済み'};
      }
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
    case 'gogoout':    reservation = parseGogoout_(body); break;
    case 'rentacar_dc': reservation = parseRentacarDC_(body); break;
    case 'rentacar_dc2': reservation = parseRentacarDC_(body); break;
  }

  if (!reservation) {
    Logger.log('Failed to parse reservation from ' + ota);
    return {type:'failure', id:'不明', ota:otaCode, name:'', reason:'パース失敗'};
  }

  // メール受信日時を予約日時として記録（LT計算の正確性のため）
  reservation._booked_at = message.getDate().toISOString();

  // Filter: 高松 only
  if (!isTakamatsuReservation_(reservation)) {
    Logger.log('Skipping non-Takamatsu: ' + reservation.id +
      ' (store=' + (reservation._store || '') + ', rawClass=' + (reservation._rawClass || '') + ')');
    return {type:'skip', id:reservation.id, reason:'他店舗'};
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

  // じゃらん事前決済: 予約登録成功後にSquareリンク作成→Slack→スプシ
  if (reservation.ota === 'J' && reservation.price > 0) {
    try {
      handleJalanPayment_(reservation);
    } catch (e) {
      Logger.log('[JalanPayment] Error: ' + e.message);
    }
  }

  if (assigned) {
    return {type:'success', id:reservation.id, ota:otaCode, name:reservation.name,
      dates:reservation.lend_date+'~'+reservation.return_date,
      vehicle:reservation.vehicle, assignedTo:assigned.name+' ('+assigned.plate_no+')'};
  } else {
    var failReason = reservation._vehicleModel
      ? '車種指定「' + reservation._vehicleModel + '」空車なし（' + reservation.vehicle + 'クラス）'
      : '配車不可（' + reservation.vehicle + 'クラス空車なし）';
    return {type:'failure', id:reservation.id, ota:otaCode, name:reservation.name,
      reason:failReason,
      dates:reservation.lend_date+'~'+reservation.return_date};
  }
}

// ============================================================
// Store / Class Filter
// ============================================================
function isTakamatsuReservation_(res) {
  var store = res._store || '';
  var rawClass = res._rawClass || '';
  var address = res._address || '';
  var delPlace = res.del_place || '';
  var colPlace = res.col_place || '';

  // 住所判定: 香川県/高松 → true
  if (/香川県|高松市|高松/.test(address)) return true;
  if (/沖縄県|那覇|北海道|札幌/.test(address)) return false;

  // 営業所名判定
  if (store.indexOf('高松') !== -1 || store.indexOf('香川') !== -1) return true;
  if (store.indexOf('那覇') !== -1 || store.indexOf('札幌') !== -1) return false;

  // お届け/回収場所判定
  if (/高松|香川|丸亀|坂出/.test(delPlace + colPlace)) return true;
  if (/那覇|沖縄|札幌|千歳/.test(delPlace + colPlace)) return false;

  // クラスコード判定: _TKM → true
  if (/_TKM/i.test(rawClass)) return true;
  if (/_OKA|_OKI|_SPK/i.test(rawClass)) return false;

  // 判定不能 → 高松として取り込む（他店GASが除外するため両方で漏れるリスクを回避）
  Logger.log('WARNING: Store undetermined, defaulting to TAKAMATSU: ' + (res.id || '?'));
  return true;
}

// TODO: 高松店のクラスが確定したら更新
function extractVehicleClass_(rawClass) {
  if (!rawClass) return '';
  // A2/B2を先にチェック
  if (/A2/i.test(rawClass)) return 'A2';
  if (/B2/i.test(rawClass)) return 'B2';

  // クラス名 + 車種名 マッピング
  var officialMap = {
    'アルファードHクラス': 'A', 'アルファードH': 'A',
    'アルファードMクラス': 'B', 'アルファードM': 'B',
    'アルファード': 'A',
    'ワンボックスB': 'B', 'ヴェルファイア': 'B',
    'セレナHクラス': 'B', 'セレナH': 'B', 'セレナ': 'B',
    'ヴォクシー': 'B',
    'ノアHクラス': 'B', 'ノアH': 'B', 'ノア': 'B',
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

// --- 補償種類の統一判定 ---
// 優先度: フル > NOC > 免責 > なし
// 全OTA共通で使う。メール本文または補償フィールドの文字列を渡す
function detectInsurance_(text) {
  if (!text) return 'なし';
  if (/フルカバー|フル補償|安心フル|あんしんフル/i.test(text)) return 'フル';
  if (/安心パック|NOC|ノンオペレーション|ノンオペ/i.test(text)) {
    if (/NOC[補償]*[：:\s]*(なし|未加入|無し|加入しない)/i.test(text)) {
    } else {
      return 'NOC';
    }
  }
  if (/レンタカー安心パック[：:\s]*あり/i.test(text)) return 'NOC';
  if (/免責補償制度\(CDW\)[：:\s]*あり/i.test(text)) return '免責';
  if (/免責補償[：:\s]*あり|免責補償制度[：:\s]*あり|免責[：:\s]*加入|免責補償料/i.test(text)) return '免責';
  if (/免責/.test(text) && !/免責[：:\s]*(なし|未加入|無し|加入しない|0円)/i.test(text)) return '免責';
  return 'なし';
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

/**
 * 車両名が指定車種に厳密マッチするか判定
 * 「プリウス」→「プリウス①」OK、「プリウスα①」NG
 * 「プリウスα」→「プリウスα①」OK、「プリウス①」NG
 * 「アルファード」→「アルファード①」OK、「アルファードM①」NG
 */
function isModelMatch_(vehicleName, preferredModel) {
  var idx = vehicleName.indexOf(preferredModel);
  if (idx === -1) return false;
  // マッチ位置の直後の文字を確認
  var afterChar = vehicleName.charAt(idx + preferredModel.length);
  // 直後が空 or 数字 or 丸数字(①-⑳) or スペース → 正しいマッチ
  // 直後がアルファベットやカタカナ → 別車種（例: プリウス→プリウスα、アルファード→アルファードM）
  if (!afterChar) return true;  // 完全一致
  if (/[\d①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳\s\/]/.test(afterChar)) return true;
  return false;
}

function padZero_(n) { return ('0' + parseInt(n, 10)).slice(-2); }
function parsePrice_(str) { if (!str) return 0; return parseInt(str.replace(/[,，円\s]/g, ''), 10) || 0; }
function cleanPhone_(str) { if (!str) return ''; return str.replace(/[^\d-]/g, '').trim(); }
function cleanName_(str) { if (!str) return ''; return str.replace(/\s*様\s*$/, '').trim(); }

/**
 * HP予約のクラス行から実際の車種名を抽出
 * 「アルファードHクラス(TOYOTA)」→「アルファード」
 * 「コンパクト(TOYOTA)」→ ''（クラス名であり車種名ではない）
 * 「ハリアー(TOYOTA)」→「ハリアー」
 */
function extractModelName_(classLine) {
  if (!classLine) return '';
  // 括弧・メーカー名を除去（全角・半角両方対応）
  var cleaned = classLine.replace(/[（(].*?[）)]/g, '').replace(/[_](ハイブリッド|HYBRID|hybrid|ガソリン|ディーゼル)$/,'').trim();
  // クラス名パターン → 車種名ではないので空を返す
  var classPatterns = [
    /^アルファード[HM]クラス/, /^(ノア|セレナ|ヴォクシー)Hクラス/,
    /^ワンボックス[BD]2?/, /^コンパクトSUV/,
    /^コンパクト$/, /^ハイブリッド$/, /^[ABCDSFH]2?クラス$/
  ];
  for (var i = 0; i < classPatterns.length; i++) {
    if (classPatterns[i].test(cleaned)) {
      // クラス名の中に車種名が含まれるケースを抽出
      var modelMap = {
        'アルファードM': 'アルファードM',  // Bクラスのアルファード（Mを先にマッチ）
        'アルファード': 'アルファード', 'ヴェルファイア': 'ヴェルファイア',
        'セレナH': 'セレナH', 'セレナ': 'セレナ',  // セレナH(Hybrid)を先にマッチ
        'ヴォクシー': 'ヴォクシー',
        'ノアH': 'ノアH', 'ノア': 'ノア',  // ノアH(Hybrid)を先にマッチ
        'ヤリスクロス': 'ヤリスクロス', 'ライズ': 'ライズ',
        'エスクァイア': 'エスクァイア',
        'ヴィッツ': 'ヴィッツ', 'ノート': 'ノート', 'アクア': 'アクア',
        'プリウスα': 'プリウスα', 'プリウスアルファ': 'プリウスアルファ', 'プリウス': 'プリウス',
        'ハリアー': 'ハリアー'
      };
      // 長い名前から優先マッチ
      var mKeys = Object.keys(modelMap).sort(function(a,b){return b.length-a.length;});
      for (var j = 0; j < mKeys.length; j++) {
        if (cleaned.indexOf(mKeys[j]) !== -1) return modelMap[mKeys[j]];
      }
      return ''; // クラス名だけで車種名なし
    }
  }
  // 再発防止: 「車種名クラス」→「クラス」を除去して車種名を返す
  if (/クラス$/.test(cleaned)) {
    var stripped = cleaned.replace(/クラス$/, '');
    if (stripped.length >= 2 && /[ァ-ヴー]/.test(stripped)) return stripped;
  }
  // クラス名パターンに該当しない → そのまま車種名として返す
  return cleaned;
}

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
  var insurance = detectInsurance_(insuranceStr);
  var peopleStr = extractField_(body, '乗車人数');
  var people = 0;
  var pM = peopleStr.match(/大人\s*(\d+)/);
  if (pM) people += parseInt(pM[1], 10);
  var cM = peopleStr.match(/子供.*?(\d+)/);
  if (cM) people += parseInt(cM[1], 10);
  // 料金内訳パース（基本料金/オプション/補償/割引）
  var basePriceJ = parsePrice_(extractField_(body, '基本料金合計'));
  var optionPriceJ = parsePrice_(extractField_(body, 'オプション料金'));
  var insurancePriceJ = parsePrice_(extractField_(body, '補償（任意加入）料金'));
  var dropOffFeeJ = parsePrice_(extractField_(body, '乗捨料金'));
  var nightFeeJ = parsePrice_(extractField_(body, '深夜手数料'));
  var couponJ = parsePrice_(extractField_(body, '利用クーポン'));
  var pointStrJ = extractField_(body, '利用ポイント');
  var pointJ = 0;
  var pointMatchJ = (pointStrJ || '').match(/(\d[\d,]*)/);
  if (pointMatchJ) pointJ = parsePrice_(pointMatchJ[1]);
  var discountJ = couponJ + pointJ;
  var base_price_j = basePriceJ;
  var option_price_j = optionPriceJ + insurancePriceJ + dropOffFeeJ + nightFeeJ;
  // 利用者への請求額（クーポン・ポイント差引後）を優先。なければ合計金額
  var billingPrice = parsePrice_(extractField_(body, '利用者への請求額'));
  var price = billingPrice > 0 ? billingPrice : parsePrice_(extractField_(body, '合計金額'));
  var arrFlight = extractField_(body, '到着便');
  var depFlight = extractField_(body, '出発便');
  var flight = [arrFlight, depFlight].filter(Boolean).join(' / ');
  // チャイルドシート検出
  var optB = 0, optC = 0, optJ2 = 0;
  var optLine = extractField_(body, 'オプション');
  if (optLine) {
    var cbM = optLine.match(/チャイルドシート\s*[x×]\s*(\d+)/i);
    if (cbM) optC = parseInt(cbM[1], 10);
    var bbM = optLine.match(/ベビーシート\s*[x×]\s*(\d+)/i);
    if (bbM) optB = parseInt(bbM[1], 10);
    var jbM = optLine.match(/ジュニアシート\s*[x×]\s*(\d+)/i);
    if (jbM) optJ2 = parseInt(jbM[1], 10);
  }
  return {
    id: id, ota: 'J', name: nameKana || name,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: price, base_price: base_price_j, option_price: option_price_j, discount: discountJ,
    status: '確定', tel: tel, mail: mail,
    flight: flight, visit_type: '', del_place: '', col_place: '',
    opt_b: optB, opt_c: optC, opt_j: optJ2,
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
      rawClass = planMatch[1] + '_TKM';
    }
  }
  var optionsStr = extractField_(body, '・オプション/車両の特徴');
  var insurance = detectInsurance_(optionsStr);
  // 料金内訳パース（楽天）
  var basePriceR = parsePrice_(extractField_(body, '・基本料金'));
  if (!basePriceR) basePriceR = parsePrice_(extractField_(body, '基本料金'));
  var insurancePriceR = parsePrice_(extractField_(body, '・免責補償料金'));
  if (!insurancePriceR) insurancePriceR = parsePrice_(extractField_(body, '免責補償料金'));
  var optionPriceR = parsePrice_(extractField_(body, '・オプション料金'));
  if (!optionPriceR) optionPriceR = parsePrice_(extractField_(body, 'オプション料金'));
  // クーポン割引（レンタカー事業者クーポン）
  // 楽天クーポン・楽天ポイントは discount に含めない（楽天側の割引であり事業者売上には影響しない）
  var couponR = parsePrice_(extractField_(body, '（レンタカー事業者クーポン利用）'));
  var discountR = couponR;
  // 差引支払金額（クーポン差引後）を優先。なければ合計金額
  var billingR = parsePrice_(extractField_(body, '（差引支払金額）'));
  var price = billingR > 0 ? billingR : parsePrice_(extractField_(body, '（合計）'));
  var base_price_r = basePriceR;
  var option_price_r = insurancePriceR + optionPriceR;
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
    price: price, base_price: base_price_r, option_price: option_price_r, discount: discountR,
    status: '確定', tel: '', mail: '',
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
  var insurance = detectInsurance_(body);
  if (insurance === 'なし' && insurancePrice > 0) insurance = '免責';
  // 料金内訳パース（skyticket）
  var basePriceS = parsePrice_(extractField_(body, '基本料金'));
  var optionPriceS = parsePrice_(extractField_(body, 'オプション料金'));
  var base_price_s = basePriceS;
  var option_price_s = insurancePrice + optionPriceS;
  return {
    id: id, ota: 'S', name: nameKana,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: totalPrice, base_price: base_price_s, option_price: option_price_s, discount: 0,
    status: '確定', tel: tel, mail: mail,
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
  // 料金内訳パース（エアトリ）
  var basePriceA = parsePrice_(extractField_(body, '基本料金'));
  if (!basePriceA) basePriceA = parsePrice_(extractField_(body, 'レンタカー料金'));
  var optionPriceA = parsePrice_(extractField_(body, 'オプション料金'));
  var insurancePriceA = parsePrice_(extractField_(body, '補償料金'));
  if (!insurancePriceA) insurancePriceA = parsePrice_(extractField_(body, '免責補償料金'));
  var base_price_a = basePriceA;
  var option_price_a = optionPriceA + insurancePriceA;
  var insuranceStr = extractField_(body, '補償オプション');
  var insurance = detectInsurance_(insuranceStr || body);
  var arrFlight = extractField_(body, '到着便');
  var depFlight = extractField_(body, '出発便');
  var flight = [arrFlight, depFlight].filter(Boolean).join(' / ');
  return {
    id: id, ota: 'O', name: nameKana,
    lend_date: lend.date, lend_time: lend.time,
    return_date: ret.date, return_time: ret.time,
    vehicle: vehicleClass, people: 0, insurance: insurance,
    price: price, base_price: base_price_a, option_price: option_price_a, discount: 0,
    status: '確定', tel: tel, mail: mail,
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
  // オフィシャル予約: 車種名指定 → クラスコード変換
  // HP予約はクラス指定ではなく車種指定。車種名を最優先でマッチさせる
  var modelToClass = {
    // Tier1: 具体的な車種名（最優先）
    'アルファードHクラス(A2)': 'A2', 'アルファードH(A2)': 'A2',
    'アルファードMクラス': 'B', 'アルファードM': 'B',  // アルファードMは必ずBクラス
    'プリウスアルファ': 'H', 'プリウスα': 'H', 'プリウス': 'H',
    'アルファードHクラス': 'A', 'アルファードH': 'A',
    'アルファード': 'A',
    'ヴェルファイア': 'B',
    'セレナHクラス': 'B', 'セレナH': 'B', 'セレナ': 'B',
    'ヴォクシー': 'B',
    'ノアHクラス': 'B', 'ノアH': 'B', 'ノア': 'B',
    'ヤリスクロス': 'C', 'ライズ': 'C',
    'エスクァイア': 'D',
    'ヴィッツ': 'F', 'ノート': 'F', 'アクア': 'F',
    'ハリアー': 'S'
  };
  var classNameToClass = {
    // Tier2: クラス名パターン（車種名で見つからなかった場合のみ使用）
    'アルファードHクラス(A2)': 'A2', 'アルファードH(A2)': 'A2',
    'アルファードMクラス': 'B', 'アルファードM': 'B',
    'アルファードHクラス': 'A', 'アルファードH': 'A',
    'ノアHクラス': 'B', 'セレナHクラス': 'B',
    'ワンボックスB2': 'B2', 'ワンボックスB': 'B',
    'コンパクトSUV': 'C', 'ワンボックスD': 'D',
    'コンパクト': 'F', 'ハイブリッド': 'H', 'ハリアー': 'S'
  };
  var vehicleClass = '';
  var classLineMatch = body.match(/ご予約車両クラス\s*\n\s*(.+)/);
  if (classLineMatch) {
    var classLine = classLineMatch[1].trim();
    // Tier1: 車種名を優先マッチ（長い名前から）
    var modelKeys = Object.keys(modelToClass).sort(function(a,b){return b.length - a.length;});
    for (var ci = 0; ci < modelKeys.length; ci++) {
      if (classLine.indexOf(modelKeys[ci]) !== -1) {
        vehicleClass = modelToClass[modelKeys[ci]];
        break;
      }
    }
    // Tier2: 車種名で見つからなければクラス名パターン
    if (!vehicleClass) {
      var classKeys = Object.keys(classNameToClass).sort(function(a,b){return b.length - a.length;});
      for (var ci2 = 0; ci2 < classKeys.length; ci2++) {
        if (classLine.indexOf(classKeys[ci2]) !== -1) {
          vehicleClass = classNameToClass[classKeys[ci2]];
          break;
        }
      }
    }
    // Tier3: どちらでも見つからなければ従来のregex
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
  var insurance = detectInsurance_(body);
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
    price: price, base_price: price, option_price: 0, discount: 0,
    status: '確定', tel: tel, mail: mail,
    flight: '', visit_type: '', del_place: delPlace, col_place: colPlace,
    opt_b: optB, opt_c: optC, opt_j: optJ,
    _store: '', _rawClass: vehicleClass, _address: address,
    _vehicleModel: classLineMatch ? extractModelName_(classLineMatch[1]) : ''
  };
}

// ============================================================
// GoGoOut Parser
// ============================================================
function parseGogoout_(body) {
  // 予約番号
  var idMatch = body.match(/予約番号[：:]\s*\n?\s*(\S+)/);
  if (!idMatch) return null;
  var id = idMatch[1].trim();

  // 利用時間・返却時間（フォーマット: 2026-07-24 18:00）
  var lendMatch = body.match(/利用時間[：:]\s*\n?\s*(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/);
  var retMatch  = body.match(/返却時間[：:]\s*\n?\s*(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/);
  if (!lendMatch || !retMatch) return null;

  // 氏名
  var nameMatch = body.match(/氏名[：:]\s*\n?\s*(.+)/);
  var name = nameMatch ? nameMatch[1].trim() : '';

  // 電話番号
  var telMatch = body.match(/携帯番号[：:]\s*\n?\s*(\S+)/);
  var tel = telMatch ? cleanPhone_(telMatch[1]) : '';

  // Email
  var mailMatch = body.match(/Email[：:]\s*\n?\s*(\S+)/);
  var mail = mailMatch ? mailMatch[1].trim() : '';

  // 車種（トヨタ｜ALPHARD → ALPHARD）
  var carMatch = body.match(/車種[：:]\s*\n?\s*(.+)/);
  var rawClass = carMatch ? carMatch[1].trim() : '';
  // 車種名からクラス判定
  var vehicleClass = '';
  if (/ALPHARD|アルファード/i.test(rawClass)) vehicleClass = 'A';
  else if (/VELLFIRE|ヴェルファイア/i.test(rawClass)) vehicleClass = 'B';
  else if (/SERENA|セレナ/i.test(rawClass)) vehicleClass = 'B';
  else if (/VOXY|ヴォクシー/i.test(rawClass)) vehicleClass = 'B';
  else if (/NOAH|ノア/i.test(rawClass)) vehicleClass = 'B';
  else if (/YARIS\s*CROSS|ヤリスクロス/i.test(rawClass)) vehicleClass = 'C';
  else if (/RAIZE|ライズ/i.test(rawClass)) vehicleClass = 'C';
  else if (/ESQUIRE|エスクァイア/i.test(rawClass)) vehicleClass = 'D';
  else if (/VITZ|ヴィッツ|NOTE|ノート|AQUA|アクア/i.test(rawClass)) vehicleClass = 'F';
  else if (/PRIUS\s*ALPHA|プリウスアルファ|プリウスα/i.test(rawClass)) vehicleClass = 'H';
  else if (/PRIUS|プリウス/i.test(rawClass)) vehicleClass = 'H';
  else if (/HARRIER|ハリアー/i.test(rawClass)) vehicleClass = 'S';
  else vehicleClass = extractVehicleClass_(rawClass);

  // 座席数
  var seatMatch = body.match(/(\d+)座席数/);
  var people = seatMatch ? parseInt(seatMatch[1], 10) : 0;

  // フライト情報
  var arrFlightMatch = body.match(/到着フライト番号[：:]\s*\n?\s*(\S+)/);
  var depFlightMatch = body.match(/復路フライト番号[：:]\s*\n?\s*(\S+)/);
  var flight = [arrFlightMatch ? arrFlightMatch[1] : '', depFlightMatch ? depFlightMatch[1] : ''].filter(Boolean).join(' / ');

  // チャイルドシート
  var optB = 0, optC = 0, optJ = 0;
  var csMatch = body.match(/チャイルドシート[^：:]*[：:]\s*\n?\s*(\d+)/);
  if (csMatch) optC = parseInt(csMatch[1], 10);
  else if (/チャイルドシート/i.test(body)) optC = 1;
  var bsMatch = body.match(/ベビーシート[^：:]*[：:]\s*\n?\s*(\d+)/);
  if (bsMatch) optB = parseInt(bsMatch[1], 10);
  var jsMatch = body.match(/ジュニアシート[^：:]*[：:]\s*\n?\s*(\d+)/);
  if (jsMatch) optJ = parseInt(jsMatch[1], 10);

  // 免責
  var insurance = detectInsurance_(body);

  // 送迎場所
  var deliveryMatch = body.match(/送迎サービス[^：:]*[：:]\s*\n?\s*(.+)/);
  var delPlace = deliveryMatch ? deliveryMatch[1].trim().replace(/\s*TWD\d+.*/, '') : '';

  // 店舗名から高松判定用
  var storeMatch = body.match(/店舗名[：:]\s*\n?\s*(.+)/);
  var store = storeMatch ? storeMatch[1].trim() : '';
  var addrMatch = body.match(/店舗住所[：:]\s*\n?\s*(.+)/);
  var address = addrMatch ? addrMatch[1].trim() : '';

  return {
    id: id, ota: 'G', name: name,
    lend_date: lendMatch[1], lend_time: lendMatch[2],
    return_date: retMatch[1], return_time: retMatch[2],
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: 0, base_price: 0, option_price: 0, discount: 0,
    status: '確定', tel: tel, mail: mail,
    flight: flight, visit_type: '', del_place: delPlace, col_place: '',
    opt_b: optB, opt_c: optC, opt_j: optJ,
    _store: store, _rawClass: rawClass, _address: address
  };
}

// ============================================================
// レンタカードットコム Parser
// ============================================================
function parseRentacarDC_(body) {
  // 予約番号
  var idMatch = body.match(/予約番号\s*[：:]\s*(\S+)/);
  if (!idMatch) return null;
  var id = idMatch[1].trim();

  // 予約者名（カナ優先）
  var kanaMatch = body.match(/予約者カナ[：:]\s*(.+)/);
  var nameMatch = body.match(/予約者名\s*[：:]\s*(.+)/);
  var name = (kanaMatch ? kanaMatch[1] : nameMatch ? nameMatch[1] : '').trim();

  // 連絡先
  var telMatch = body.match(/電話番号\s*[：:]\s*([\d-]+)/);
  var tel = telMatch ? cleanPhone_(telMatch[1]) : '';
  var mailMatch = body.match(/メールアドレス[：:]\s*(\S+)/);
  var mail = mailMatch ? mailMatch[1].trim() : '';

  // 貸出日・時間（別フィールド: 「貸出日：」「貸出時間：」）
  var ldMatch = body.match(/貸出日[^時]*[：:]\s*(\d{4}\/\d{1,2}\/\d{1,2})/);
  var ltMatch = body.match(/貸出時間\s*[：:]\s*(\d{1,2}:\d{2})/);
  if (!ldMatch) return null;
  var lendDate = ldMatch[1].replace(/\//g, '-');
  var lendTime = ltMatch ? ltMatch[1] : '';

  // 返却日・時間
  var rdMatch = body.match(/返却日\s*[：:]\s*(\d{4}\/\d{1,2}\/\d{1,2})/);
  var rtMatch = body.match(/返却時間\s*[：:]\s*(\d{1,2}:\d{2})/);
  if (!rdMatch) return null;
  var returnDate = rdMatch[1].replace(/\//g, '-');
  var returnTime = rtMatch ? rtMatch[1] : '';

  // 店舗名（高松判定用）
  var storeMatch = body.match(/貸出店舗名[：:]\s*(.+)/);
  var store = storeMatch ? storeMatch[1].trim() : '';

  // 車両クラス判定（プラン名 + 車種名）
  var planMatch = body.match(/プラン名\s*[：:]\s*(.+)/);
  var carMatch = body.match(/車種名\s*[：:]\s*(.+)/);
  var rawPlan = planMatch ? planMatch[1].trim() : '';
  var rawCar = carMatch ? carMatch[1].trim() : '';
  var rawClass = rawPlan + ' ' + rawCar;

  var vehicleClass = '';
  if (/ALPHARD|アルファード/i.test(rawClass)) vehicleClass = 'A';
  else if (/VELLFIRE|ヴェルファイア/i.test(rawClass)) vehicleClass = 'B';
  else if (/SERENA|セレナ/i.test(rawClass)) vehicleClass = 'B';
  else if (/VOXY|ヴォクシー/i.test(rawClass)) vehicleClass = 'B';
  else if (/NOAH|ノア/i.test(rawClass)) vehicleClass = 'B';
  else if (/YARIS\s*CROSS|ヤリスクロス/i.test(rawClass)) vehicleClass = 'C';
  else if (/RAIZE|ライズ/i.test(rawClass)) vehicleClass = 'C';
  else if (/ESQUIRE|エスクァイア/i.test(rawClass)) vehicleClass = 'D';
  else if (/VITZ|ヴィッツ|NOTE|ノート|AQUA|アクア/i.test(rawClass)) vehicleClass = 'F';
  else if (/PRIUS\s*ALPHA|プリウスアルファ|プリウスα/i.test(rawClass)) vehicleClass = 'H';
  else if (/PRIUS|プリウス/i.test(rawClass)) vehicleClass = 'H';
  else if (/HARRIER|ハリアー/i.test(rawClass)) vehicleClass = 'S';
  else vehicleClass = extractVehicleClass_(rawClass);

  // 人数
  var adultMatch = body.match(/大人\s*[：:]\s*(\d+)\s*名/);
  var childMatch = body.match(/子供\s*[：:]\s*(\d+)\s*名/);
  var people = (adultMatch ? parseInt(adultMatch[1], 10) : 0) + (childMatch ? parseInt(childMatch[1], 10) : 0);

  // 料金（￥35.500 形式: ピリオドが千区切り）
  var priceMatch = body.match(/合計料金\s*[：:]\s*[￥¥]?([\d.]+)/);
  var price = 0;
  if (priceMatch) {
    price = parseInt(priceMatch[1].replace(/\./g, ''), 10) || 0;
  }

  // 免責
  var cdwMatch = body.match(/免責料金\s*[：:]\s*[￥¥]?([\d.]+)/);
  var cdwPrice = cdwMatch ? parseInt(cdwMatch[1].replace(/\./g, ''), 10) : 0;
  var insurance = detectInsurance_(body);
  if (insurance === 'なし' && cdwPrice > 0) insurance = '免責';

  // フライト情報
  var arrFlight = '';
  var depFlight = '';
  var arrMatch2 = body.match(/現地到着[^：:]*[：:]+\s*.*?([A-Z]{2}\d{2,5})/i);
  var depMatch2 = body.match(/現地出発[^：:]*[：:]+\s*.*?([A-Z]{2}\d{2,5})/i);
  if (arrMatch2) arrFlight = arrMatch2[1];
  if (depMatch2) depFlight = depMatch2[1];
  var flight = [arrFlight, depFlight].filter(Boolean).join(' / ');

  // チャイルドシート
  var optB = 0, optC = 0, optJ = 0;
  var optsText = body.match(/オプション[：:]\s*(.+)/);
  var optsStr = optsText ? optsText[1] : '';
  var csMatch = optsStr.match(/チャイルドシート[^：:]*[：:]?\s*(\d+)/);
  if (csMatch) optC = parseInt(csMatch[1], 10);
  var bsMatch = optsStr.match(/ベビーシート[^：:]*[：:]?\s*(\d+)/);
  if (bsMatch) optB = parseInt(bsMatch[1], 10);
  var jsMatch = optsStr.match(/ジュニアシート[^：:]*[：:]?\s*(\d+)/);
  if (jsMatch) optJ = parseInt(jsMatch[1], 10);

  // 来店/デリバリー判定
  var visitType = '来店';

  Logger.log('[RC-PARSE] id=' + id + ' name=' + name + ' class=' + vehicleClass +
    ' plan=' + rawPlan + ' car=' + rawCar + ' price=' + price + ' flight=' + flight);

  // 料金内訳パース（レンタカードットコム）
  var basePriceRC = parsePrice_(extractField_(body, '基本料金'));
  var optionPriceRC = parsePrice_(extractField_(body, 'オプション料金'));
  var base_price_rc = basePriceRC || price;
  var option_price_rc = cdwPrice + optionPriceRC;
  return {
    id: id, ota: 'RC', name: name,
    lend_date: lendDate, lend_time: lendTime,
    return_date: returnDate, return_time: returnTime,
    vehicle: vehicleClass, people: people, insurance: insurance,
    price: price, base_price: base_price_rc, option_price: option_price_rc, discount: 0,
    status: '確定', tel: tel, mail: mail,
    flight: flight, visit_type: visitType, del_place: '', col_place: '',
    opt_b: optB, opt_c: optC, opt_j: optJ,
    _store: store, _rawClass: rawClass
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

  // Delete fleet + tasks, update reservation status to cancelled
  deleteFromFleet_(reservationId);
  deleteFromTasks_(reservationId);
  supabaseUpdate_('tkm_reservations', 'id=eq.' + encodeURIComponent(reservationId), {status: 'cancelled'});

  // じゃらん事前決済: キャンセル連動
  try {
    handleJalanPaymentCancel_(reservationId);
  } catch (e) {
    Logger.log('[JalanPaymentCancel] Error: ' + e.message);
  }

  Logger.log('Cancelled reservation: ' + reservationId);
  return reservationId;
}

// ============================================================
// Supabase API
// ============================================================
function supabaseHeaders_() {
  return {
    'apikey': getSupabaseKey_(),
    'Authorization': 'Bearer ' + getSupabaseKey_(),
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
}

function supabaseGet_(table, queryParams) {
  // Supabase REST APIはデフォルト1000件制限。必要に応じてlimitを明示的に付与
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
  var rows = supabaseGet_('tkm_reservations', 'id=eq.' + encodeURIComponent(reservationId) + '&select=id,status');
  return rows.length > 0 ? rows[0] : null;
}

// GAS内部フィールド → tkm_reservationsカラム名変換
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
    base_price: reservation.base_price || 0,
    option_price: reservation.option_price || 0,
    discount: reservation.discount || 0,
    tel: reservation.tel || '',
    mail: reservation.mail || '',
    del_flight: reservation.flight || '',
    del_place: reservation.del_place || '',
    col_place: reservation.col_place || '',
    car_seat: String(reservation.opt_c || 0),
    junior_seat: String(reservation.opt_j || 0),
    visit_type: reservation.visit_type || '',
    status: 'confirmed',
    booked_at: reservation._booked_at || null
  };
  return row;
}

// キャンセル済み予約を再有効化（同一IDで取り直しされた場合）
function reactivateReservation_(reservationId, reservation) {
  var row = toDbRow_(reservation);
  row.status = 'confirmed';
  var ok = supabaseUpdate_('tkm_reservations', 'id=eq.' + encodeURIComponent(reservationId), row);
  if (ok) Logger.log('Reactivated cancelled reservation: ' + reservationId);
  return ok;
}

function insertReservation_(reservation) {
  var row = toDbRow_(reservation);
  var result = supabasePost_('tkm_reservations', row);
  if (result) Logger.log('Inserted reservation: ' + reservation.id);
  return result;
}

function deleteReservation_(reservationId) {
  return supabaseDelete_('tkm_reservations', 'id=eq.' + encodeURIComponent(reservationId));
}

function deleteFromFleet_(reservationId) {
  return supabaseDelete_('tkm_fleet', 'reservation_id=eq.' + encodeURIComponent(reservationId));
}

function deleteFromTasks_(reservationId) {
  return supabaseDelete_('tkm_tasks', 'reservation_id=eq.' + encodeURIComponent(reservationId));
}

// ============================================================
// Vehicle Auto-Assignment
// ============================================================
function autoAssignVehicle_(reservation) {
  var vehicleClass = reservation.vehicle;
  if (!vehicleClass) {
    Logger.log('No vehicle class for ' + reservation.id + '. Will be unassigned.');
    return;
  }

  // A2→A, B2→Bフォールバック（同じ車種構成のため）
  var searchClass = vehicleClass;
  if (vehicleClass === 'A2') searchClass = 'A';
  if (vehicleClass === 'B2') searchClass = 'B';

  var vehicles = supabaseGet_('tkm_vehicles',
    'type=eq.' + encodeURIComponent(searchClass) + '&insurance_veh=eq.false&select=code,name,plate_no,seats');
  if (vehicles.length === 0) {
    Logger.log('No vehicles of class ' + searchClass + ' (original: ' + vehicleClass + '). ' + reservation.id + ' will be unassigned.');
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

  // 車種名指定がある場合、指定車種のみ検索
  var preferredModel = reservation._vehicleModel || '';
  var assignedVehicle = null;
  if (preferredModel) {
    for (var i = 0; i < vehicles.length; i++) {
      var v = vehicles[i];
      if (busyVehicleCodes[v.code]) continue;
      if (isModelMatch_(v.name, preferredModel)) {
        assignedVehicle = v;
        break;
      }
    }
    if (assignedVehicle) {
      Logger.log('Preferred model match: ' + preferredModel + ' -> ' + assignedVehicle.code);
    } else {
      // HP予約の車種指定車両が全て塞がっている → 未配車にする（フォールバック禁止）
      Logger.log('Preferred model "' + preferredModel + '" not available for ' + reservation.id +
        '. Will be unassigned (vehicle model priority).');
      return null;
    }
  }
  // 指定車種なし（OTA予約 or クラス名のみ指定）→ クラス内の先頭空車
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
      ' (' + lendDate + '~' + returnDate + '). ' + reservation.id + ' will be unassigned.');
    return null;
  }

  // INSERT直前の最終重複チェック（二重配車防止ガード）
  var finalCheck = getOverlappingFleetVehicles_(lendDate, returnDate);
  if (finalCheck.indexOf(assignedVehicle.code) >= 0) {
    Logger.log('FINAL GUARD: ' + assignedVehicle.code + ' has become busy between check and insert. ' +
      reservation.id + ' will be unassigned.');
    return null;
  }

  var fleetRow = { reservation_id: reservation.id, vehicle_code: assignedVehicle.code };
  var result = supabasePost_('tkm_fleet', fleetRow);
  if (result) {
    Logger.log('Assigned ' + assignedVehicle.code + ' (' + assignedVehicle.name + ') to ' + reservation.id);
    return assignedVehicle;
  }
  return null;
}

function getOverlappingFleetVehicles_(lendDate, returnDate) {
  // DB側で期間重複を絞り込む
  var query = 'select=vehicle_code,reservation_id,tkm_reservations!inner(start_date,end_date)' +
    '&tkm_reservations.start_date=lte.' + encodeURIComponent(returnDate) +
    '&tkm_reservations.end_date=gte.' + encodeURIComponent(lendDate);
  var overlapping = supabaseGet_('tkm_fleet', query);
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
  return supabaseGet_('tkm_maintenance', query);
}

// ============================================================
// Slack Notifications (メール転送方式)
// ============================================================
function sendSlackSuccess_(items) {
  var SLACK_EMAIL = getSlackEmail_();
  if (!SLACK_EMAIL) { Logger.log('[Slack] SLACK_EMAIL not set.'); return; }
  var lines = ['高松店新規予約取込完了通知', ''];
  items.forEach(function(r) {
    lines.push('[' + r.ota + '] ' + r.id);
    lines.push('  ' + r.name + ' / ' + r.dates + ' / ' + r.vehicle + 'クラス');
    lines.push('  -> 配車: ' + r.assignedTo);
    lines.push('');
  });
  lines.push('合計: ' + items.length + '件');
  try { MailApp.sendEmail(SLACK_EMAIL, lines[0], lines.join('\n')); } catch (e) { Logger.log('[Slack] Send error: ' + e.message); }
}

function sendSlackFailure_(items) {
  var SLACK_EMAIL = getSlackEmail_();
  if (!SLACK_EMAIL) { Logger.log('[Slack] SLACK_EMAIL not set.'); return; }
  var lines = ['高松店新規予約取込失敗通知', ''];
  items.forEach(function(r) {
    lines.push('[' + r.ota + '] ' + (r.id || '不明'));
    if (r.name) lines.push('  ' + r.name + (r.dates ? ' / ' + r.dates : ''));
    lines.push('  理由: ' + r.reason);
    lines.push('');
  });
  lines.push('合計: ' + items.length + '件 ※手動対応が必要です');
  try { MailApp.sendEmail(SLACK_EMAIL, lines[0], lines.join('\n')); } catch (e) { Logger.log('[Slack] Send error: ' + e.message); }
}

function sendSlackCancel_(items) {
  var SLACK_EMAIL = getSlackEmail_();
  if (!SLACK_EMAIL) { Logger.log('[Slack] SLACK_EMAIL not set.'); return; }
  var lines = ['高松店予約キャンセル処理通知', ''];
  items.forEach(function(r) {
    lines.push('[' + r.ota + '] ' + r.id + ' -> キャンセル処理完了');
  });
  lines.push('');
  lines.push('合計: ' + items.length + '件');
  try { MailApp.sendEmail(SLACK_EMAIL, lines[0], lines.join('\n')); } catch (e) { Logger.log('[Slack] Send error: ' + e.message); }
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
    var options = {
      method: 'post',
      headers: {
        'apikey': getSupabaseKey_(),
        'Authorization': 'Bearer ' + getSupabaseKey_(),
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    };
    UrlFetchApp.fetch(getSupabaseUrl_() + '/rest/v1/tkm_app_settings', options);
    Logger.log('[Heartbeat] Updated: ' + key);
  } catch (e) {
    Logger.log('[Heartbeat] Error: ' + e.message);
  }
}

// 監視チェック: 30分間隔で実行。ハートビートが途絶えていたらSlack通知
function checkHeartbeats() {
  var checks = [
    { key: 'tkm_gas_email', label: '高松GAS予約取込', thresholdMin: 30 }
  ];

  checks.forEach(function(check) {
    try {
      var url = getSupabaseUrl_() + '/rest/v1/tkm_app_settings?key=eq.heartbeat_' + check.key + '&select=value';
      var options = {
        method: 'get',
        headers: {
          'apikey': getSupabaseKey_(),
          'Authorization': 'Bearer ' + getSupabaseKey_()
        },
        muteHttpExceptions: true
      };
      var res = UrlFetchApp.fetch(url, options);
      var data = JSON.parse(res.getContentText());
      var props = PropertiesService.getScriptProperties();

      if (!data || data.length === 0) {
        var initKey = 'alert_init_' + check.key;
        if (!props.getProperty(initKey)) {
          sendSlackAlert_('WARNING: ' + check.label + ': ハートビート未登録（初回実行待ち）');
          props.setProperty(initKey, 'true');
        }
        return;
      }

      var hb = JSON.parse(data[0].value);
      var lastRun = new Date(hb.last_run);
      var now = new Date();
      var diffMin = Math.round((now - lastRun) / 60000);

      var alertKey = 'alert_sent_' + check.key;
      var alertSent = props.getProperty(alertKey);

      if (diffMin > check.thresholdMin) {
        if (!alertSent) {
          var timeStr = Utilities.formatDate(lastRun, 'Asia/Tokyo', 'MM/dd HH:mm');
          sendSlackAlert_('ALERT: ' + check.label + ' が' + diffMin + '分間停止中\n最終実行: ' + timeStr + '\n処理数: ' + (hb.processed || 0) + '件 / エラー: ' + (hb.errors || 0) + '件');
          props.setProperty(alertKey, 'true');
        }
      } else {
        // 復旧検知
        if (alertSent) {
          sendSlackAlert_('OK: ' + check.label + ' 復旧しました（停止' + diffMin + '分）');
          props.deleteProperty(alertKey);
        }
      }
    } catch (e) {
      Logger.log('[checkHeartbeats] Error for ' + check.key + ': ' + e.message);
    }
  });
}

function sendSlackAlert_(message) {
  var SLACK_EMAIL = getSlackEmail_();
  if (!SLACK_EMAIL) { Logger.log('[Alert] SLACK_EMAIL not set.'); return; }
  try {
    MailApp.sendEmail(SLACK_EMAIL, message.split('\n')[0], message);
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
  var reserveKeywords = ['予約確定', '予約通知', '予約受付', '新規予約', 'ご予約完了', '予約を受け付け', '予約登録'];
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
  var checkedKey = 'tkm_unknown_senders_alerted';
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

      if (alerted[msgId]) continue;

      var isKnown = knownSenders.some(function(s) { return from.indexOf(s) !== -1; });
      if (isKnown) continue;

      var hasReserveKeyword = reserveKeywords.some(function(kw) { return subject.indexOf(kw) !== -1; });
      if (!hasReserveKeyword) continue;

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
    var lines = ['WARNING: 高松店 未知の予約メール検知 ' + unknowns.length + '件', ''];
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

    try {
      PropertiesService.getScriptProperties().setProperty(checkedKey, JSON.stringify(alerted));
    } catch (e) {}
  }
}

// セットアップ: 監視トリガー追加（30分間隔）
function setupMonitoring() {
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
// Message-Level Processed ID Management
// ============================================================
var PROCESSED_MSG_KEY = 'tkm_processed_msg_ids';

function getProcessedMsgIds_() {
  try {
    var raw = PropertiesService.getScriptProperties().getProperty(PROCESSED_MSG_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (e) {
    Logger.log('[ProcessedMsgIds] Read error: ' + e.message);
    return {};
  }
}

function saveProcessedMsgIds_(ids) {
  try {
    PropertiesService.getScriptProperties().setProperty(PROCESSED_MSG_KEY, JSON.stringify(ids));
  } catch (e) {
    Logger.log('[ProcessedMsgIds] Save error: ' + e.message);
  }
}

/**
 * 一括再スキャン: processed_takamatsuラベル済みスレッドから未処理のキャンセルを検出・処理
 */
function rescanLabeledForMissedCancellations() {
  var label = getOrCreateLabel_(LABEL_NAME);
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') label:' + LABEL_NAME + ' newer_than:7d';
  var threads = GmailApp.search(query, 0, 100);
  Logger.log('[Rescan] Found ' + threads.length + ' labeled thread(s) to scan.');

  var fixed = [];
  for (var i = 0; i < threads.length; i++) {
    var messages = threads[i].getMessages();
    for (var j = 0; j < messages.length; j++) {
      var msg = messages[j];
      var subject = msg.getSubject();

      var isCxl = CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; });
      if (!isCxl) continue;

      var body = msg.getPlainBody();
      var from = msg.getFrom();

      var ota = null;
      var otaKeys = Object.keys(OTA_SENDERS);
      for (var k = 0; k < otaKeys.length; k++) {
        if (from.indexOf(OTA_SENDERS[otaKeys[k]]) !== -1) { ota = otaKeys[k]; break; }
      }
      if (!ota) continue;

      var resId = (ota === 'rakuten') ? extractField_(body, '・予約番号') : extractField_(body, '予約番号');
      if (!resId) continue;

      var existing = reservationExists_(resId);
      if (!existing || existing.status === 'cancelled') continue;

      Logger.log('[Rescan] Found missed cancellation: ' + resId + ' (' + ota + ')');
      deleteFromFleet_(resId);
      deleteFromTasks_(resId);
      supabaseUpdate_('tkm_reservations', 'id=eq.' + encodeURIComponent(resId), {status: 'cancelled'});
      fixed.push(resId);
      Logger.log('[Rescan] Cancelled: ' + resId);
    }
  }

  if (fixed.length > 0) {
    var msg = '[Rescan] 未処理キャンセル ' + fixed.length + '件を修正\n' + fixed.join(', ');
    sendSlackAlert_(msg);
    Logger.log('[Rescan] Fixed ' + fixed.length + ' missed cancellations: ' + fixed.join(', '));
  } else {
    Logger.log('[Rescan] No missed cancellations found.');
  }
}

// ============================================================
// Gmail Helpers
// ============================================================
function markAllExistingAsProcessed() {
  var label = getOrCreateLabel_(LABEL_NAME);
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') -label:' + LABEL_NAME;
  var threads = GmailApp.search(query, 0, 500);
  Logger.log('Marking ' + threads.length + ' threads as ' + LABEL_NAME);
  for (var i = 0; i < threads.length; i++) {
    threads[i].addLabel(label);
  }
  Logger.log('Done. All existing emails marked as ' + LABEL_NAME + '.');
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
 * 指定予約IDのDB+配車状態をSlackに通知する（手動確認用）
 */
function notifyReservations() {
  var targetIds = ['CHANGE_ME'];
  var items = [];
  var failures = [];
  for (var i = 0; i < targetIds.length; i++) {
    var id = targetIds[i];
    var res = supabaseGet_('tkm_reservations', 'id=eq.' + encodeURIComponent(id) + '&select=*');
    if (res.length === 0) {
      failures.push({id: id, ota: '?', name: '', reason: 'DB未登録'});
      continue;
    }
    var r = res[0];
    var fleet = supabaseGet_('tkm_fleet', 'reservation_id=eq.' + encodeURIComponent(id) + '&select=vehicle_code');
    var assignedTo = '未配車';
    if (fleet.length > 0) {
      var veh = supabaseGet_('tkm_vehicles', 'code=eq.' + encodeURIComponent(fleet[0].vehicle_code) + '&select=name,plate_no');
      if (veh.length > 0) {
        assignedTo = veh[0].name + ' (' + veh[0].plate_no + ')';
      } else {
        assignedTo = fleet[0].vehicle_code;
      }
    }
    items.push({
      id: id, ota: r.ota, name: r.name,
      dates: r.start_date + '~' + r.end_date,
      vehicle: r.vehicle_class,
      assignedTo: assignedTo
    });
  }
  if (items.length > 0) sendSlackSuccess_(items);
  if (failures.length > 0) sendSlackFailure_(failures);
  Logger.log('notifyReservations: ' + items.length + ' sent, ' + failures.length + ' failed');
}

/**
 * 指定した予約IDのメールを再検索し、再処理する。
 * - DB未登録 → メールを再パース → insert → 自動配車
 * - DB登録済み＆vehicle_class空 → メールからクラス取得 → DB更新 → 自動配車
 * - FORCE_REPARSE=true → fleet削除→再パース→料金内訳含むDB更新→再配車
 */
function reprocessByIds() {
  var targetIds = ['CHANGE_ME'];
  var FORCE_REPARSE = true; // DB登録済みだがbase_price/option_price未設定 → 再パース→内訳更新
  var label = getOrCreateLabel_(LABEL_NAME);
  var successes = [];
  var failures = [];

  for (var t = 0; t < targetIds.length; t++) {
    var targetId = targetIds[t];
    Logger.log('=== Reprocessing: ' + targetId + ' (FORCE=' + FORCE_REPARSE + ') ===');

    // 1. DB状態チェック
    var existing = reservationExists_(targetId);

    if (existing && existing.status !== 'cancelled') {
      // DB登録済み
      var fullRes = supabaseGet_('tkm_reservations', 'id=eq.' + encodeURIComponent(targetId) + '&select=*');
      if (fullRes.length === 0) {
        failures.push({id: targetId, ota: '?', name: '', reason: 'DB参照失敗'});
        continue;
      }
      var dbRow = fullRes[0];

      // FORCE_REPARSE モード: fleet削除→メール再パース→DB更新→再配車
      if (FORCE_REPARSE) {
        Logger.log(targetId + ' FORCE mode: deleting fleet and re-parsing email...');
        deleteFromFleet_(targetId);
        var emailDataF = findEmailByReservationId_(targetId);
        if (!emailDataF) {
          failures.push({id: targetId, ota: dbRow.ota || '?', name: dbRow.name || '', reason: 'メール検索失敗(FORCE)'});
          continue;
        }
        var parsedF = emailDataF.parsed;
        if (!parsedF || !parsedF.vehicle) {
          failures.push({id: targetId, ota: dbRow.ota || '?', name: dbRow.name || '', reason: 'クラス抽出失敗(FORCE)'});
          continue;
        }
        Logger.log(targetId + ' re-parsed class: ' + parsedF.vehicle + ' (was: ' + dbRow.vehicle_class + ')');
        // DB更新（クラス・車種モデル・料金内訳・保険・オプション）
        var updateFields = {vehicle_class: parsedF.vehicle};
        if (parsedF.base_price > 0 || parsedF.option_price > 0) {
          updateFields.base_price = parsedF.base_price || 0;
          updateFields.option_price = parsedF.option_price || 0;
          updateFields.discount = parsedF.discount || 0;
          updateFields.price = parsedF.price || dbRow.price;
          updateFields.amount = parsedF.price || dbRow.price;
        }
        if (parsedF.insurance) updateFields.insurance = parsedF.insurance;
        if (parsedF.opt_b > 0) updateFields.car_seat = String(parsedF.opt_b);
        if (parsedF.opt_c > 0) updateFields.car_seat = String(parsedF.opt_c);
        if (parsedF.opt_j > 0) updateFields.junior_seat = String(parsedF.opt_j);
        if (parsedF.flight) updateFields.del_flight = parsedF.flight;
        supabaseUpdate_('tkm_reservations', 'id=eq.' + encodeURIComponent(targetId), updateFields);
        // 再配車
        var fakeResF = {
          id: targetId, vehicle: parsedF.vehicle,
          lend_date: dbRow.start_date, return_date: dbRow.end_date,
          name: dbRow.name, ota: dbRow.ota,
          _vehicleModel: parsedF._vehicleModel || ''
        };
        var assignedF = autoAssignVehicle_(fakeResF);
        if (assignedF) {
          successes.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
            dates: dbRow.start_date + '~' + dbRow.end_date,
            vehicle: parsedF.vehicle, assignedTo: assignedF.name + ' (' + assignedF.plate_no + ')'});
        } else {
          failures.push({id: targetId, ota: dbRow.ota, name: dbRow.name,
            reason: '配車不可（' + parsedF.vehicle + 'クラス空車なし）',
            dates: dbRow.start_date + '~' + dbRow.end_date});
        }
        continue;
      }

      if (dbRow.vehicle_class && dbRow.vehicle_class !== '') {
        Logger.log(targetId + ' already has vehicle_class=' + dbRow.vehicle_class + '. Checking fleet...');
        var fleetCheck = supabaseGet_('tkm_fleet', 'reservation_id=eq.' + encodeURIComponent(targetId) + '&select=vehicle_code');
        if (fleetCheck.length > 0) {
          Logger.log(targetId + ' already assigned to ' + fleetCheck[0].vehicle_code + '. Skipping.');
          continue;
        }
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
      supabaseUpdate_('tkm_reservations', 'id=eq.' + encodeURIComponent(targetId),
        {vehicle_class: parsed.vehicle});
      Logger.log('Updated vehicle_class=' + parsed.vehicle + ' for ' + targetId);

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

    // 2. DB未登録またはキャンセル済み → メール再取得＆処理
    Logger.log(targetId + ' not in DB (or cancelled). Searching email...');
    var emailData2 = findEmailByReservationId_(targetId);
    if (!emailData2) {
      failures.push({id: targetId, ota: '?', name: '', reason: 'メール未発見'});
      continue;
    }

    try {
      emailData2.thread.removeLabel(label);
      Logger.log('Removed ' + LABEL_NAME + ' label from thread for ' + targetId);
    } catch (e) {
      Logger.log('Label removal warning: ' + e.message);
    }

    var result = processMessage_(emailData2.message, false);

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
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ') ' + reservationId;
  var threads = GmailApp.search(query, 0, 10);

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
      if (CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; })) continue;

      var from = msg.getFrom();
      var ota = null;
      var otaKeys = Object.keys(OTA_SENDERS);
      for (var k = 0; k < otaKeys.length; k++) {
        if (from.indexOf(OTA_SENDERS[otaKeys[k]]) !== -1) { ota = otaKeys[k]; break; }
      }
      if (!ota) continue;

      var parsed = null;
      switch (ota) {
        case 'jalan':      parsed = parseJalan_(body); break;
        case 'rakuten':    parsed = parseRakuten_(body); break;
        case 'skyticket':  parsed = parseSkyticket_(body); break;
        case 'airtrip':    parsed = parseAirtrip_(body); break;
        case 'airtrip_dp': parsed = parseAirtrip_(body); break;
        case 'official':   parsed = parseOfficial_(body); break;
        case 'gogoout':    parsed = parseGogoout_(body); break;
        case 'rentacar_dc': parsed = parseRentacarDC_(body); break;
        case 'rentacar_dc2': parsed = parseRentacarDC_(body); break;
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

// ============================================================
// booked_at バックフィル（一回限り手動実行）
// tkm_reservations.booked_at が null の行をGmailメール受信日時で埋める
// ============================================================
function backfillBookedAt() {
  var fromClause = Object.values(OTA_SENDERS).map(function(s) { return 'from:' + s; }).join(' OR ');
  var query = '(' + fromClause + ')';

  var allMessages = [];
  var start = 0;
  var batchSize = 100;
  while (true) {
    var threads = GmailApp.search(query, start, batchSize);
    if (threads.length === 0) break;
    for (var i = 0; i < threads.length; i++) {
      var msgs = threads[i].getMessages();
      for (var j = 0; j < msgs.length; j++) {
        allMessages.push(msgs[j]);
      }
    }
    if (threads.length < batchSize) break;
    start += batchSize;
  }

  Logger.log('[backfill] Total messages: ' + allMessages.length);

  var updated = 0, skipped = 0, errors = 0;

  for (var k = 0; k < allMessages.length; k++) {
    var msg = allMessages[k];
    var from = msg.getFrom();
    var subject = msg.getSubject();
    var body = msg.getPlainBody();
    var msgDate = msg.getDate();

    var isCancellation = CANCEL_KEYWORDS.some(function(kw) { return subject.indexOf(kw) !== -1; });
    if (isCancellation) continue;

    var ota = null;
    var otaKeys = Object.keys(OTA_SENDERS);
    for (var oi = 0; oi < otaKeys.length; oi++) {
      if (from.indexOf(OTA_SENDERS[otaKeys[oi]]) !== -1) { ota = otaKeys[oi]; break; }
    }
    if (!ota) continue;

    if (!OTA_RESERVE_SUBJECTS[ota] || subject.indexOf(OTA_RESERVE_SUBJECTS[ota]) === -1) continue;

    var reservationId = extractReservationId_(ota, body);
    if (!reservationId) continue;

    var bookedAtStr = Utilities.formatDate(msgDate, 'Asia/Tokyo', "yyyy-MM-dd'T'HH:mm:ssXXX");
    var patchUrl = getSupabaseUrl_() + '/rest/v1/tkm_reservations'
      + '?id=eq.' + encodeURIComponent(reservationId)
      + '&booked_at=is.null';
    var options = {
      method: 'PATCH',
      headers: {
        'apikey': getSupabaseKey_(),
        'Authorization': 'Bearer ' + getSupabaseKey_(),
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      payload: JSON.stringify({ booked_at: bookedAtStr }),
      muteHttpExceptions: true
    };

    try {
      var resp = UrlFetchApp.fetch(patchUrl, options);
      var arr = JSON.parse(resp.getContentText());
      if (arr && arr.length > 0) {
        Logger.log('[backfill] Updated: ' + reservationId + ' -> ' + bookedAtStr);
        updated++;
      } else {
        skipped++;
      }
    } catch (e) {
      Logger.log('[backfill] Error ' + reservationId + ': ' + e.message);
      errors++;
    }

    if (k > 0 && k % 50 === 0) Utilities.sleep(1000);
  }

  Logger.log('[backfill] Done. updated=' + updated + ' skipped=' + skipped + ' errors=' + errors);
}

// 予約IDを本文から抽出（バックフィル用）
function extractReservationId_(ota, body) {
  if (ota === 'rakuten') {
    return extractField_(body, '・予約番号');
  }
  return extractField_(body, '予約番号');
}

// ============================================================
// じゃらん事前決済（高松店版 — 札幌店GASより移植）
// ============================================================

var JALAN_PAY_CHANNEL = 'PLACEHOLDER_JALAN_PAY_CHANNEL';  // TODO: 高松店用Slackチャンネルを設定
var PAYMENT_SHEET_ID = 'PLACEHOLDER_PAYMENT_SHEET_ID';     // TODO: 高松店用スプレッドシートIDを設定
var SQUARE_LOCATION_ID = 'L8N7J9RKPN3WH';

function getSlackBotToken_() { return PropertiesService.getScriptProperties().getProperty('SLACK_BOT_TOKEN'); }
function getSquareToken_() { return PropertiesService.getScriptProperties().getProperty('SQUARE_API_TOKEN'); }

// Square Payment Links API で決済リンクを直接作成
function createSquarePaymentLink_(itemName, amount) {
  var token = getSquareToken_();
  if (!token) { Logger.log('[Square] No SQUARE_API_TOKEN'); return null; }
  try {
    var resp = UrlFetchApp.fetch('https://connect.squareup.com/v2/online-checkout/payment-links', {
      method: 'post',
      headers: {'Authorization':'Bearer '+token, 'Content-Type':'application/json', 'Square-Version':'2024-01-18'},
      payload: JSON.stringify({
        idempotency_key: Utilities.getUuid(),
        quick_pay: {
          name: itemName,
          price_money: {amount: amount, currency: 'JPY'},
          location_id: SQUARE_LOCATION_ID
        }
      }),
      muteHttpExceptions: true
    });
    var data = JSON.parse(resp.getContentText());
    if (data.payment_link && data.payment_link.url) {
      Logger.log('[Square] Link created: ' + data.payment_link.url);
      return data.payment_link.url;
    }
    Logger.log('[Square] API error: ' + resp.getContentText());
    return null;
  } catch (e) { Logger.log('[Square] Exception: ' + e.message); return null; }
}

function handleJalanPayment_(reservation) {
  var resId = reservation.id;

  var existing = supabaseGet_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(resId) + '&select=id');
  if (existing && existing.length > 0) { Logger.log('[JalanPayment] Already exists: ' + resId); return; }

  // 1. Square決済リンクを直接作成
  var lendShort = (reservation.lend_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
  var retShort = (reservation.return_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
  var itemName = (reservation.name||'') + '様 じゃらん事前決済(' + lendShort + '-' + retShort + ')';
  var payUrl = createSquarePaymentLink_(itemName, reservation.price||0);

  if (!payUrl) {
    // Square API失敗 → status='new'で保存（checkSquareLinksでリトライ）+ Slack障害通知
    var payData = {reservation_id:resId, customer_name:reservation.name, customer_email:reservation.mail||'', amount:reservation.price||0, status:'new', lend_date:reservation.lend_date, return_date:reservation.return_date, vehicle_class:reservation.vehicle||''};
    supabasePost_('tkm_jalan_payments', payData);
    postToSlackChannel_(JALAN_PAY_CHANNEL, '🔴 *Squareリンク作成失敗*\n予約番号： ' + resId + '\n宛名： ' + reservation.name + '\n金額： ¥' + (reservation.price||0) + '\n→ checkSquareLinksトリガーでリトライします');
    Logger.log('[JalanPayment] Square link creation failed, saved as new: ' + resId);
    return;
  }

  // 2. DB保存（link_created状態で即保存）
  var now = new Date().toISOString();
  var payData = {reservation_id:resId, customer_name:reservation.name, customer_email:reservation.mail||'', amount:reservation.price||0, status:'link_created', square_payment_url:payUrl, link_created_at:now, lend_date:reservation.lend_date, return_date:reservation.return_date, vehicle_class:reservation.vehicle||''};
  var inserted = supabasePost_('tkm_jalan_payments', payData);
  if (!inserted) { Logger.log('[JalanPayment] DB insert failed: ' + resId); return; }
  Logger.log('[JalanPayment] Created with link: ' + resId + ' ¥' + reservation.price + ' → ' + payUrl);

  // 3. Slack投稿（リンク付きで可視化）
  var slackText = '💳 *じゃらん事前決済*\n利用店舗： 高松店\n予約番号： ' + resId + '\n宛名： ' + reservation.name + '\n品目： じゃらん事前決済(' + lendShort + '-' + retShort + ')\n金額： ¥' + (reservation.price||0).toLocaleString() + '\nSquareリンク： ' + payUrl;
  var slackTs = postToSlackChannel_(JALAN_PAY_CHANNEL, slackText);
  if (slackTs) {
    supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(resId), {slack_ts: slackTs});
  }

  // 4. スプレッドシートに記録
  appendToPaymentSheet_({reservation_id:resId, customer_name:reservation.name, amount:reservation.price||0, lend_date:reservation.lend_date, return_date:reservation.return_date, slack_ts:slackTs||''}, payUrl);
}

function handleJalanPaymentCancel_(reservationId) {
  var rows = supabaseGet_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(reservationId) + '&select=id,status,amount,customer_name');
  if (!rows || rows.length === 0) return;
  var pay = rows[0];
  var prevStatus = pay.status;
  if (prevStatus === 'cancelled' || prevStatus === 'refund' || prevStatus === 'refunded') { Logger.log('[JalanPaymentCancel] Already cancelled/refunded: ' + reservationId); return; }
  var now = new Date().toISOString();
  if (prevStatus === 'paid') {
    supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(reservationId), {status:'refund', cancelled_at:now});
    updatePaymentSheetStatus_(reservationId, '⚠️ 要返金', '');
    postToSlackChannel_(JALAN_PAY_CHANNEL, '⚠️ *返金対応必要*\n予約番号： ' + reservationId + '\n宛名： ' + (pay.customer_name||'') + '\n金額： ¥' + (pay.amount||0) + '\n状態： 入金済みキャンセル → *要Square返金*');
  } else {
    supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(reservationId), {status:'cancelled', cancelled_at:now});
    updatePaymentSheetStatus_(reservationId, '❌ キャンセル', '');
    postToSlackChannel_(JALAN_PAY_CHANNEL, '🔄 *キャンセル（決済前）*\n予約番号： ' + reservationId + '\n宛名： ' + (pay.customer_name||'') + '\n金額： ¥' + (pay.amount||0) + '\n状態： 未入金キャンセル・対応不要');
  }
  Logger.log('[JalanPaymentCancel] Done: ' + reservationId + ' → ' + (prevStatus === 'paid' ? 'refund' : 'cancelled'));
}

function postToSlackChannel_(channel, text) {
  var token = getSlackBotToken_();
  if (!token) { Logger.log('[Slack] No SLACK_BOT_TOKEN configured'); return null; }
  try {
    var resp = UrlFetchApp.fetch('https://slack.com/api/chat.postMessage', {method:'post', headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json'}, payload:JSON.stringify({channel:channel, text:text}), muteHttpExceptions:true});
    var data = JSON.parse(resp.getContentText());
    if (data.ok) return data.ts;
    Logger.log('[Slack] Post error: ' + data.error);
    return null;
  } catch (e) { Logger.log('[Slack] Exception: ' + e.message); return null; }
}

function getSlackThreadReplies_(channel, ts) {
  var token = getSlackBotToken_();
  if (!token) return [];
  try {
    var resp = UrlFetchApp.fetch('https://slack.com/api/conversations.replies?channel=' + channel + '&ts=' + ts, {method:'get', headers:{'Authorization':'Bearer '+token}, muteHttpExceptions:true});
    var data = JSON.parse(resp.getContentText());
    return data.ok ? (data.messages||[]) : [];
  } catch (e) { Logger.log('[Slack] Thread read error: ' + e.message); return []; }
}

function checkSquareLinks() {
  var rows = supabaseGet_('tkm_jalan_payments', 'status=in.(new,link_created)&select=reservation_id,customer_name,customer_email,amount,status,slack_ts,lend_date,return_date,square_payment_url');
  if (!rows || rows.length === 0) return;
  for (var i = 0; i < rows.length; i++) {
    var pay = rows[i];

    // status=new: handleJalanPayment_でSquareリンク作成が失敗した行 → リトライ
    if (pay.status === 'new') {
      var lendShort = (pay.lend_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
      var retShort = (pay.return_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
      var itemName = (pay.customer_name||'') + '様 じゃらん事前決済(' + lendShort + '-' + retShort + ')';
      var payUrl = createSquarePaymentLink_(itemName, pay.amount||0);
      if (!payUrl) { Logger.log('[checkSquareLinks] Retry failed: ' + pay.reservation_id); continue; }
      var now = new Date().toISOString();
      supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(pay.reservation_id), {square_payment_url:payUrl, status:'link_created', link_created_at:now});
      Logger.log('[checkSquareLinks] Retry success: ' + pay.reservation_id + ' → ' + payUrl);
      // Slack投稿（リトライ成功通知）
      var slackText = '💳 *じゃらん事前決済（リトライ成功）*\n予約番号： ' + pay.reservation_id + '\n宛名： ' + pay.customer_name + '\n金額： ¥' + (pay.amount||0).toLocaleString() + '\nSquareリンク： ' + payUrl;
      var slackTs = postToSlackChannel_(JALAN_PAY_CHANNEL, slackText);
      if (slackTs && !pay.slack_ts) { supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(pay.reservation_id), {slack_ts: slackTs}); }
      appendToPaymentSheet_(pay, payUrl);
      pay.square_payment_url = payUrl; pay.status = 'link_created';
    }

    // status=link_created: メール送信
    if (pay.status === 'link_created' && pay.square_payment_url && pay.customer_email) {
      var sent = sendJalanPaymentEmail_(pay);
      if (sent) {
        supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(pay.reservation_id), {status:'email_sent', email_sent_at:new Date().toISOString()});
        postToSlackChannel_(JALAN_PAY_CHANNEL, '📧 *メール送信完了*\n予約番号： ' + pay.reservation_id + '\n宛名： ' + pay.customer_name + '\n金額： ¥' + pay.amount);
        Logger.log('[checkSquareLinks] Email sent: ' + pay.reservation_id);
      }
    }
  }
}

function sendJalanPaymentEmail_(pay) {
  if (!pay || !pay.customer_email || !pay.square_payment_url) { Logger.log('[JalanPayment] Email BLOCKED: missing data'); return; }
  try {
    // --- TODO: 以下のプレースホルダーをGASスクリプトプロパティまたは定数で設定 ---
    var LINE_URL = PropertiesService.getScriptProperties().getProperty('LINE_URL') || 'https://lin.ee/XXXXXXXXX';
    var LINE_ID = PropertiesService.getScriptProperties().getProperty('LINE_ID') || '@xxxxx';
    var STORE_TEL = PropertiesService.getScriptProperties().getProperty('STORE_TEL') || '000-0000-0000';
    var STORE_EMAIL = PropertiesService.getScriptProperties().getProperty('STORE_EMAIL') || 'reserve@buddica-takamatsu.jp';

    var subject = '【BUDDICA高松店】事前決済のご案内（予約番号: ' + pay.reservation_id + '）';

    var body = '';
    body += pay.customer_name + ' 様\n\n';
    body += 'この度はBUDDICA高松店をご予約いただき、\n';
    body += '誠にありがとうございます。\n\n';
    body += '下記の手順に沿って、ご出発準備をお願いいたします。\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '■ ご予約内容\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '予約番号: ' + pay.reservation_id + '\n';
    body += '貸出日: ' + (pay.lend_date || '') + '\n';
    body += '返却日: ' + (pay.return_date || '') + '\n';
    body += '車両クラス: ' + (pay.vehicle_class || '') + '\n';
    body += 'お支払い金額: ¥' + Number(pay.amount||0).toLocaleString() + '\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '■ STEP1: LINE友だち追加（必須）\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '当日のお届け場所のご連絡・変更連絡は\n';
    body += 'すべてLINEで行っております。\n\n';
    body += '▼ 友だち追加はこちら\n';
    body += LINE_URL + '\n\n';
    body += 'LINE ID: ' + LINE_ID + '\n\n';
    body += '※ 追加後、「お名前」「予約番号」をメッセージでお送りください。\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '■ STEP2: 事前決済（必須）\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += 'お支払い金額: ¥' + Number(pay.amount||0).toLocaleString() + '\n\n';
    body += '▼ お支払いはこちら（クレジットカード）\n';
    body += pay.square_payment_url + '\n\n';
    body += '※ ご出発3日前の19:00までにお支払いください。\n';
    body += '※ 期限を過ぎますとご予約をキャンセルさせていただく\n';
    body += '  場合がございます。\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '■ 当日の流れ（デリバリー）\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '1. LINEでお届け場所・時間を確認\n';
    body += '2. スタッフがご指定場所へお車をお届け\n';
    body += '3. 免許証確認・車両説明後、ご出発\n';
    body += '4. ご返却もご指定場所でOK\n\n';
    body += '※ 高松市内のホテル・駅・空港等へお届け可能です。\n';
    body += '※ お届け場所はLINEでご相談ください。\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '■ ご注意事項\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += '・当店はデリバリー専門のレンタカーです。\n';
    body += '  実店舗での受け渡しは行っておりません。\n';
    body += '・免許証（原本）を必ずご持参ください。\n';
    body += '・キャンセル・変更はLINEまたはお電話にて\n';
    body += '  ご連絡ください。\n\n';

    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';
    body += 'BUDDICA高松店（デリバリーレンタカー）\n';
    body += 'TEL: ' + STORE_TEL + '\n';
    body += 'LINE: ' + LINE_ID + '\n';
    body += '営業時間: 9:00〜19:00（年中無休）\n';
    body += '━━━━━━━━━━━━━━━━━━━━━━━━\n';

    GmailApp.sendEmail(pay.customer_email, subject, body, {name: 'BUDDICA高松店', replyTo: STORE_EMAIL});
    return true;
  } catch (e) { Logger.log('[JalanPaymentEmail] Error: ' + e.message); return false; }
}

// 入金確認 v3（Payment Links APIベース）
function checkPaymentStatus() {
  var ss = SpreadsheetApp.openById(PAYMENT_SHEET_ID);
  var sheet = ss.getSheetByName('支払い管理');
  if (!sheet) { Logger.log('[PaymentStatus] Sheet not found'); return; }
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  var data = sheet.getRange(2, 1, lastRow - 1, 14).getValues();
  var unpaidRows = [];
  for (var i = 0; i < data.length; i++) {
    var status = String(data[i][8] || '');
    var url = String(data[i][7] || '');
    var store = String(data[i][2] || '');
    if (status.indexOf('済') === -1 && status.indexOf('キャンセル') === -1 && url) {
      unpaidRows.push({rowIndex:i+2, reservationId:String(data[i][3]||'').trim(), customerName:String(data[i][4]||'').replace(/様$/,'').trim(), amount:Number(data[i][6])||0, url:url.trim(), media:String(data[i][13]||'').trim(), store:store.trim(), orderId:null});
    }
  }
  if (unpaidRows.length === 0) { Logger.log('[PaymentStatus] No unpaid rows found'); return; }
  Logger.log('[PaymentStatus] Checking ' + unpaidRows.length + ' unpaid rows');
  var token = getSquareToken_();
  if (!token) { Logger.log('[PaymentStatus] No SQUARE_API_TOKEN'); postToSlackChannel_(JALAN_PAY_CHANNEL, '🔴 *入金確認システム障害*\nSQUARE_API_TOKENが未設定です。'); return; }
  var linkMap = fetchPaymentLinkMap_(token);
  var linkMapSize = linkMap ? Object.keys(linkMap).length : 0;
  if (linkMapSize === 0) { Logger.log('[PaymentStatus] CRITICAL: Payment Links map is empty'); postToSlackChannel_(JALAN_PAY_CHANNEL, '🔴 *入金確認システム障害*\nSquare Payment Links APIが0件を返しました。\n`debugPaymentV3` を手動実行して診断してください。'); return; }
  var orderIdsToCheck = [], unmatchedRows = [];
  for (var i = 0; i < unpaidRows.length; i++) {
    var normalizedUrl = normalizeSquareUrl_(unpaidRows[i].url);
    var orderId = linkMap[normalizedUrl];
    if (orderId) { unpaidRows[i].orderId = orderId; orderIdsToCheck.push(orderId); }
    else { unmatchedRows.push(unpaidRows[i].reservationId); Logger.log('[PaymentStatus] No URL match for ' + unpaidRows[i].reservationId); }
  }
  if (orderIdsToCheck.length === 0) { postToSlackChannel_(JALAN_PAY_CHANNEL, '🔴 *入金確認システム障害*\nPayment Linksは'+linkMapSize+'件取得できましたが、スプシURLが1件もマッチしません。\n対象: ' + unmatchedRows.join(', ')); return; }
  var orderMap = batchRetrieveOrders_(token, orderIdsToCheck);
  if (!orderMap || Object.keys(orderMap).length === 0) { postToSlackChannel_(JALAN_PAY_CHANNEL, '🔴 *入金確認システム障害*\nSquare Orders取得が0件です。'); return; }
  var paidCount = 0;
  for (var i = 0; i < unpaidRows.length; i++) {
    var pay = unpaidRows[i];
    if (!pay.orderId) continue;
    try {
      var matched = isOrderPaid_(orderMap[pay.orderId]);
      if (matched) {
        var paidDateStr = Utilities.formatDate(new Date(matched.paid_at), 'Asia/Tokyo', 'yyyy/MM/dd');
        sheet.getRange(pay.rowIndex, 9).setValue('✅ 入金済み');
        sheet.getRange(pay.rowIndex, 10).setValue(paidDateStr);
        sheet.getRange(pay.rowIndex, 11).setValue(matched.order_id);
        try { supabaseUpdate_('tkm_jalan_payments', 'reservation_id=eq.' + encodeURIComponent(pay.reservationId) + '&status=neq.paid', {status:'paid', paid_at:matched.paid_at}); } catch(e) {}
        postToSlackChannel_(JALAN_PAY_CHANNEL, '✅ *入金確認完了*\n予約番号： ' + pay.reservationId + '\n宛名： ' + pay.customerName + '\n金額： ¥' + pay.amount.toLocaleString() + (pay.media ? '\n媒体： ' + pay.media : '') + '\n店舗： 高松店');
        Logger.log('[PaymentStatus] Paid: ' + pay.reservationId);
        paidCount++;
      }
    } catch (e) { Logger.log('[PaymentStatus] Error checking ' + pay.reservationId + ': ' + e.message); }
  }
  Logger.log('[PaymentStatus] Done. ' + paidCount + '/' + unpaidRows.length + ' confirmed paid');
}

function fetchPaymentLinkMap_(token) {
  var map = {}, cursor = null, fetched = 0;
  do {
    var apiUrl = 'https://connect.squareup.com/v2/online-checkout/payment-links?limit=100';
    if (cursor) apiUrl += '&cursor=' + encodeURIComponent(cursor);
    try {
      var resp = UrlFetchApp.fetch(apiUrl, {method:'get', headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Square-Version':'2024-01-18'}, muteHttpExceptions:true});
      if (resp.getResponseCode() !== 200) { Logger.log('[PaymentLinks] API error ' + resp.getResponseCode()); break; }
      var data = JSON.parse(resp.getContentText());
      (data.payment_links||[]).forEach(function(link) {
        if (link.order_id) {
          if (link.url) map[normalizeSquareUrl_(link.url)] = link.order_id;
          if (link.long_url) map[normalizeSquareUrl_(link.long_url)] = link.order_id;
        }
      });
      fetched += (data.payment_links||[]).length;
      cursor = data.cursor;
    } catch (e) { Logger.log('[PaymentLinks] Fetch error: ' + e.message); break; }
  } while (cursor && fetched < 200);
  Logger.log('[PaymentLinks] Total map entries: ' + Object.keys(map).length);
  return map;
}

function normalizeSquareUrl_(url) { return String(url||'').trim().replace(/\/+$/,'').toLowerCase(); }

function batchRetrieveOrders_(token, orderIds) {
  var map = {}, unique = [], seen = {};
  orderIds.forEach(function(id) { if (!seen[id]) { unique.push(id); seen[id]=true; } });
  for (var i = 0; i < unique.length; i += 100) {
    try {
      var resp = UrlFetchApp.fetch('https://connect.squareup.com/v2/orders/batch-retrieve', {method:'post', headers:{'Authorization':'Bearer '+token,'Content-Type':'application/json','Square-Version':'2024-01-18'}, payload:JSON.stringify({location_id:SQUARE_LOCATION_ID, order_ids:unique.slice(i,i+100)}), muteHttpExceptions:true});
      (JSON.parse(resp.getContentText()).orders||[]).forEach(function(o) { map[o.id]=o; });
    } catch (e) { Logger.log('[BatchOrders] Error: ' + e.message); }
  }
  Logger.log('[BatchOrders] Retrieved ' + Object.keys(map).length + '/' + unique.length + ' orders');
  return map;
}

function isOrderPaid_(order) {
  if (!order || !order.tenders || order.tenders.length === 0) return null;
  var netDue = order.net_amount_due_money;
  if (netDue && netDue.amount !== 0) return null;
  return {paid_at: order.tenders[0].created_at, order_id: order.id};
}

function checkUnpaidAlert() {
  var ss = SpreadsheetApp.openById(PAYMENT_SHEET_ID);
  var sheet = ss.getSheetByName('支払い管理');
  if (!sheet) return;
  var lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  var data = sheet.getRange(2, 1, lastRow - 1, 14).getValues();
  var now = new Date(), alerts = [];
  for (var i = 0; i < data.length; i++) {
    var status = String(data[i][8]||'');
    if (status.indexOf('済')!==-1 || status.indexOf('キャンセル')!==-1) continue;
    var resvId = String(data[i][3]||'').trim(), name = String(data[i][4]||'').trim(), amount = Number(data[i][6])||0, url = String(data[i][7]||'').trim();
    if (!resvId || !url) continue;
    var resv = supabaseGet_('tkm_reservations', 'id=eq.' + encodeURIComponent(resvId) + '&select=start_date');
    var lendDate = (resv && resv.length > 0 && resv[0].start_date) ? resv[0].start_date : null;
    if (!lendDate) { var dm = String(data[i][5]||'').match(/(\d{2})\/(\d{2})/); if (dm) lendDate = now.getFullYear() + '-' + dm[1] + '-' + dm[2]; }
    if (!lendDate) continue;
    var diffDays = Math.floor((new Date(lendDate+'T00:00:00+09:00') - now) / 86400000);
    if (diffDays <= 3) alerts.push({reservationId:resvId, customerName:name, amount:amount, lendDate:lendDate, daysLeft:diffDays});
  }
  if (alerts.length === 0) return;
  var lines = ['🚨 *未入金アラート* ' + alerts.length + '件\n'];
  alerts.forEach(function(a) {
    var urgency = a.daysLeft<=0 ? '🔴期限超過' : a.daysLeft<=1 ? '🟠明日出発' : '🟡'+a.daysLeft+'日後';
    lines.push('• ' + a.reservationId + ' ' + a.customerName + ' ¥' + a.amount + '（出発: ' + a.lendDate + ' ' + urgency + '）');
  });
  lines.push('\n期限超過・要電話確認');
  postToSlackChannel_(JALAN_PAY_CHANNEL, lines.join('\n'));
  Logger.log('[UnpaidAlert] ' + alerts.length + '件通知');
}

function appendToPaymentSheet_(pay, payUrl) {
  try {
    var ss = SpreadsheetApp.openById(PAYMENT_SHEET_ID);
    var sheet = ss.getSheetByName('支払い管理');
    if (!sheet) return;
    var lastRow = sheet.getLastRow();
    if (lastRow >= 2) {
      var existingIds = sheet.getRange(2, 4, lastRow-1, 1).getValues();
      for (var i = 0; i < existingIds.length; i++) { if (String(existingIds[i][0]).trim() === pay.reservation_id) { Logger.log('[Sheet] Already exists: ' + pay.reservation_id); return; } }
    }
    var lendShort = (pay.lend_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
    var retShort = (pay.return_date||'').replace(/^\d{4}-/,'').replace(/-/g,'/');
    sheet.appendRow([lastRow, Utilities.formatDate(new Date(),'Asia/Tokyo','yyyy/MM/dd'), '高松店', pay.reservation_id, (pay.customer_name||'')+'様', 'じゃらん事前決済('+lendShort+'-'+retShort+')', pay.amount||0, payUrl||pay.square_payment_url||'', '⏳ 未払い', '', '', pay.slack_ts||'', JALAN_PAY_CHANNEL||'', 'じゃらん']);
    Logger.log('[Sheet] Appended: ' + pay.reservation_id);
  } catch (e) { Logger.log('[Sheet] Append error: ' + e.message); }
}

function updatePaymentSheetStatus_(reservationId, newStatus, paidDate) {
  try {
    var ss = SpreadsheetApp.openById(PAYMENT_SHEET_ID);
    var sheet = ss.getSheetByName('支払い管理');
    if (!sheet) return;
    var lastRow = sheet.getLastRow();
    if (lastRow < 2) return;
    var resIds = sheet.getRange(2, 4, lastRow-1, 1).getValues();
    for (var i = 0; i < resIds.length; i++) {
      if (String(resIds[i][0]).trim() === reservationId) {
        sheet.getRange(i+2, 9).setValue(newStatus);
        if (paidDate) sheet.getRange(i+2, 10).setValue(Utilities.formatDate(new Date(paidDate), 'Asia/Tokyo', 'yyyy/MM/dd'));
        Logger.log('[Sheet] Status updated: ' + reservationId + ' → ' + newStatus);
        return;
      }
    }
  } catch (e) { Logger.log('[Sheet] Status update error: ' + e.message); }
}
