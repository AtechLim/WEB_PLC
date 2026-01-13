/*
   ESP32 S3 WEB PLC 메인 프로그램 (AP모드)
   - 웹 기반 PLC 래더로직 실행/저장/배포 (LittleFS + AsyncWebServer + WebSocket)
   - AP 모드 + 웹서버, LED 상태 반영, PLC Scan 루프
   - 상태(RUN/STOP) 영구 저장(Preferences) : 재부팅 후에도 유지

   ESP32_PLC_Simplified.ino의 래더 로직을 통합한 버전
*/

#include <ArduinoJson.h>
#include <Arduino.h>
#include <WiFi.h>
#include <AsyncTCP.h>
#include <ESPAsyncWebServer.h>
#include <FS.h>
#include <LittleFS.h>
#include <FastLED.h>
#define FASTLED_ESP32_I2S true  // RMT 대신 I2S 사용으로 충돌 방지
#include <Preferences.h>

// ============ CONFIGURATION ============
// WiFi 설정을 여기서 수정하세요
#define WIFI_SSID "ESP32_WEB_PLC"
#define WIFI_PASSWORD "12345678"

// Note: STA auto-connect macros removed — STA is managed via HTTP endpoints from web UI

#define WS_PORT 80

#define MAX_M_BITS 200
#define MAX_D_WORDS 100
#define MAX_I_BITS 100
#define MAX_Q_BITS 100
#define MAX_TIMERS 10
#define MAX_COUNTERS 10
#define MAX_NODES 100
#define MAX_LINKS 200

// ============ ENUMS ============
enum PLCStatus { STOP, RUN, ERROR, RESET };
enum NodeType {
  NODE_OPEN, NODE_CLOSE, NODE_RISING, NODE_FALLING, NODE_INVERT,
  NODE_COIL, NODE_SET, NODE_RESET, NODE_INSTRUCTION, NODE_NETWORK
};

// ============ STRUCTURES ============
struct Node {
  int id;
  char networkId[16];  // Network address (e.g., "N0", "N1") for execution ordering
  NodeType type;
  char addr[16];
  char tag[16];
  uint32_t setpoint;
  char instruction[16];
  char args[32];
  int x, y;
};

struct Link {
  int fromNode;
  int toNode;
  char fromPort[8];
  char toPort[8];
};

struct Timer {
  char name[16];
  unsigned long startTime;
  unsigned long preset;
  unsigned long remaining;
  bool enabled;
  bool q;
  int mode; // 0=unknown,1=TON,2=TOFF,3=TP
};

struct Counter {
  char name[16];
  int current;
  int preset;
  bool q;
};

// ============ GLOBAL OBJECTS ============
// WebSocket and Web Server
AsyncWebServer server(80);
AsyncWebSocket ws("/ws");

// PLC Status
PLCStatus plcStatus = STOP;
String plcErrorMsg = "";

// Memory from ESP32_PLC_Simplified.ino
bool mBits[MAX_M_BITS];
uint32_t dWords[MAX_D_WORDS];
bool iBits[MAX_I_BITS];
bool qBits[MAX_Q_BITS];

// Logic from ESP32_PLC_Simplified.ino
Node nodes[MAX_NODES];
Link links[MAX_LINKS];
bool nodeOutputs[MAX_NODES];
bool nodePrevOutputs[MAX_NODES];
bool nodeInputs[MAX_NODES];
bool nodePrevContactValues[MAX_NODES]; // 이전 주소값 (Rising/Falling edge 감지용)
bool nodePrevSignalIn[MAX_NODES];      // SET/RESET의 이전 입력값 (한 번만 실행)
bool nodePrevInputs[MAX_NODES];        // 이전 스캔의 nodeInputs (rising-edge detection)
int nodeCount = 0;
int linkCount = 0;

// Timers
Timer timers[MAX_TIMERS];
int timerCount = 0;

// Counters
Counter counters[MAX_COUNTERS];
int counterCount = 0;

// Timing
unsigned long lastScanTime = 0;
const unsigned long SCAN_INTERVAL = 10;

// WiFi STA 상태 (managed by /wifi/connect and /wifi/disconnect handlers)
bool staConnected = false;
IPAddress staIP;

// WiFi 스캔 상태 관리
bool wifiScanInProgress = false;
int lastScanResultCount = 0;

// JSON document for logic
DynamicJsonDocument logicDoc(8192);

// Preferences for persistence
Preferences prefs;

// LED settings
#define PLC_LED_PIN 21
#define PLC_NUM_LEDS 1
CRGB plc_leds[PLC_NUM_LEDS];

// sendPlcStatus rate-limit
unsigned long lastStatusSent = 0;
const unsigned long statusIntervalMs = 200; // 200ms min interval (5Hz)

bool ledUpdateNeeded = false; // LED 업데이트 플래그

// ============ MEMORY FUNCTIONS ============
void initMemory() {
  memset(mBits, 0, sizeof(mBits));
  memset(dWords, 0, sizeof(dWords));
  memset(iBits, 0, sizeof(iBits));
  memset(qBits, 0, sizeof(qBits));
  memset(nodePrevContactValues, 0, sizeof(nodePrevContactValues));
  memset(nodeInputs, 0, sizeof(nodeInputs));
  memset(nodeOutputs, 0, sizeof(nodeOutputs));
  memset(nodePrevOutputs, 0, sizeof(nodePrevOutputs));
  memset(nodePrevSignalIn, 0, sizeof(nodePrevSignalIn));
  memset(nodePrevInputs, 0, sizeof(nodePrevInputs));
  memset(nodePrevInputs, 0, sizeof(nodePrevInputs));
  
  // 타이머와 카운터 초기화
  timerCount = 0;
  counterCount = 0;
  memset(timers, 0, sizeof(timers));
  memset(counters, 0, sizeof(counters));
}

String toUpper(String str) {
  str.toUpperCase();
  return str;
}

bool getMBit(String addr) {
  String upper = toUpper(addr);
  if (upper.startsWith("M")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_M_BITS) {
      return mBits[index];
    }
  }
  return false;
}

void setMBit(String addr, bool value) {
  String upper = toUpper(addr);
  if (upper.startsWith("M")) {
    String numStr = upper.substring(1);
    numStr.trim();
    if (numStr.length() == 0) {
      Serial.printf("[WARN %lu] setMBit: empty index in addr '%s' - ignored\n", millis(), upper.c_str());
      return;
    }
    bool allDigits = true;
    for (size_t i = 0; i < (size_t)numStr.length(); ++i) {
      if (!isDigit(numStr.charAt(i))) { allDigits = false; break; }
    }
    if (!allDigits) {
      Serial.printf("[WARN %lu] setMBit: non-numeric index in addr '%s' - ignored\n", millis(), upper.c_str());
      return;
    }
    int index = numStr.toInt();
    if (index >= 0 && index < MAX_M_BITS) {
      mBits[index] = value;
    } else {
      Serial.printf("[WARN %lu] setMBit: index out of range %d for addr '%s' - ignored\n", millis(), index, upper.c_str());
    }
  }
}

bool getIBit(String addr) {
  String upper = toUpper(addr);
  if (upper.startsWith("I")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_I_BITS) {
      return iBits[index];
    }
  }
  return false;
}

void setIBit(String addr, bool value) {
  String upper = toUpper(addr);
  if (upper.startsWith("I")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_I_BITS) {
      iBits[index] = value;
    }
  }
}

bool getQBit(String addr) {
  String upper = toUpper(addr);
  if (upper.startsWith("Q")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_Q_BITS) {
      return qBits[index];
    }
  }
  return false;
}

void setQBit(String addr, bool value) {
  String upper = toUpper(addr);
  if (upper.startsWith("Q")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_Q_BITS) {
      qBits[index] = value;
    }
  }
}

uint32_t getDWord(String addr) {
  String upper = toUpper(addr);
  if (upper.startsWith("D")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_D_WORDS) {
      return dWords[index];
    }
  }
  return 0;
}

void setDWord(String addr, uint32_t value) {
  String upper = toUpper(addr);
  if (upper.startsWith("D")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_D_WORDS) {
      dWords[index] = value;
    }
  }
}

bool getBitFromWord(String addr) {
  // Parse D10.3 format (bit 3 of word D10)
  int dotIndex = addr.indexOf('.');
  if (dotIndex > 0) {
    String wordAddr = addr.substring(0, dotIndex);
    int bit = addr.substring(dotIndex + 1).toInt();
    uint32_t word = getDWord(wordAddr);
    return (word >> bit) & 1;
  }
  return false;
}

void setBitFromWord(String addr, bool value) {
  // Parse D10.3 format
  int dotIndex = addr.indexOf('.');
  if (dotIndex > 0) {
    String wordAddr = addr.substring(0, dotIndex);
    int bit = addr.substring(dotIndex + 1).toInt();
    uint32_t word = getDWord(wordAddr);

    if (value) {
      word |= (1 << bit);
    } else {
      word &= ~(1 << bit);
    }
    setDWord(wordAddr, word);
  }
}

void resetMemory() {
  // 기본 메모리 클리어
  memset(mBits, 0, sizeof(mBits));
  memset(dWords, 0, sizeof(dWords));
  memset(qBits, 0, sizeof(qBits));  // Q 출력도 초기화
  memset(iBits, 0, sizeof(iBits));  // I 입력도 초기화

  // 타이머 초기화 및 제거
  for (int i = 0; i < timerCount; i++) {
    timers[i].startTime = 0;
    timers[i].preset = 0;
    timers[i].enabled = false;
    timers[i].q = false;
    timers[i].name[0] = '\0';
  }
  timerCount = 0;

  // 카운터 초기화 및 제거
  for (int i = 0; i < counterCount; i++) {
    counters[i].current = 0;
    counters[i].preset = 0;
    counters[i].q = false;
    counters[i].name[0] = '\0';
  }
  counterCount = 0;

  Serial.printf("[DBG %lu] resetMemory: memory, timers and counters cleared\n", millis());
}

// ============ LOGIC FUNCTIONS ============
bool evaluateContact(String addr) {
  String upper = toUpper(addr);

  if (upper.indexOf('.') > 0) {
    return getBitFromWord(upper);
  }

  if (upper.startsWith("I")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_I_BITS) return iBits[index];
  } else if (upper.startsWith("Q")) {
    int index = upper.substring(1).toInt();
    if (index >= 0 && index < MAX_Q_BITS) return qBits[index];
  } else if (upper.startsWith("M")) {
    return getMBit(upper);
  } else if (upper.startsWith("D")) {
    return getDWord(upper) != 0;
  } else if (upper.startsWith("T")) {
    // Timer output
    for (int i = 0; i < timerCount; i++) {
      if (String(timers[i].name) == upper) {
        return timers[i].q;
      }
    }
  } else if (upper.startsWith("C")) {
    // Counter output
    for (int i = 0; i < counterCount; i++) {
      if (String(counters[i].name) == upper) {
        return counters[i].q;
      }
    }
  }

  return false;
}

bool evaluateNode(Node& node) {
  switch(node.type) {
    case NODE_NETWORK:
      return true;
    case NODE_OPEN:
      return nodeInputs[node.id] && evaluateContact(node.addr);
    case NODE_CLOSE:
      return nodeInputs[node.id] && !evaluateContact(node.addr);
    case NODE_INVERT:
      return nodeInputs[node.id] && !evaluateContact(node.addr);
    case NODE_RISING: {
      bool current = evaluateContact(node.addr);
      bool prev = nodePrevContactValues[node.id];
      nodePrevContactValues[node.id] = current;
      return nodeInputs[node.id] && (!prev && current);
    }
    case NODE_FALLING: {
      bool current = evaluateContact(node.addr);
      bool prev = nodePrevContactValues[node.id];
      nodePrevContactValues[node.id] = current;
      return nodeInputs[node.id] && (prev && !current);
    }
    case NODE_COIL:
        return nodeInputs[node.id];
      case NODE_SET:
        return nodeInputs[node.id];
      case NODE_RESET:
        return nodeInputs[node.id];
    case NODE_INSTRUCTION:
      // For instructions like TON, return the timer output
      if (String(node.instruction) == "TON" || String(node.instruction) == "TOFF" || String(node.instruction) == "TP") {
        String args = String(node.args);
        int colonIndex = args.indexOf(':');
        if (colonIndex > 0) {
          String timerName = args.substring(0, colonIndex);
          for (int i = 0; i < timerCount; i++) {
            if (String(timers[i].name) == timerName) {
              return timers[i].q;
            }
          }
        }
      } else if (String(node.instruction) == "CTU" || String(node.instruction) == "CTD") {
        String args = String(node.args);
        int colonIndex = args.indexOf(':');
        if (colonIndex > 0) {
          String counterName = args.substring(0, colonIndex);
          for (int i = 0; i < counterCount; i++) {
            if (String(counters[i].name) == counterName) {
              return counters[i].q;
            }
          }
        }
      } else if (String(node.instruction) == "EQ" || String(node.instruction) == "GT" || String(node.instruction) == "GE" ||
                 String(node.instruction) == "LT" || String(node.instruction) == "LE" || String(node.instruction) == "NE") {
        // Comparison instructions return their result directly
        return nodeOutputs[node.id];
      }
      return false;
    default:
      return false;
  }
}

void executeCoil(Node& node) {
  bool signalIn = nodeOutputs[node.id];
  bool prevSignalIn = nodePrevSignalIn[node.id];
  String upper = toUpper(node.addr);

  if (node.type == NODE_COIL) {
    if (upper.startsWith("M")) {
      setMBit(upper, signalIn);
    } else if (upper.startsWith("Q")) {
      int index = upper.substring(1).toInt();
      if (index >= 0 && index < MAX_Q_BITS) qBits[index] = signalIn;
    } else if (upper.startsWith("D")) {
      if (upper.indexOf('.') > 0) {
        setBitFromWord(upper, signalIn);
      } else {
        setDWord(upper, signalIn ? 1 : 0);
      }
    }
  } else if (node.type == NODE_SET) {
    // SET: level-triggered set (while input true, set the target; does not clear on release)
    if (signalIn) {
      if (upper.startsWith("M")) {
        setMBit(upper, true);
      } else if (upper.startsWith("Q")) {
        int index = upper.substring(1).toInt();
        if (index >= 0 && index < MAX_Q_BITS) {
          qBits[index] = true;
        }
      } else if (upper.startsWith("D")) {
        if (upper.indexOf('.') > 0) {
          setBitFromWord(upper, true);
        } else {
          setDWord(upper, 1);
        }
      }
    }
  } else if (node.type == NODE_RESET) {
    // RESET: level-triggered reset (while input true, clear the target)
    if (signalIn) {
      if (upper.startsWith("M")) {
        setMBit(upper, false);
      } else if (upper.startsWith("Q")) {
        int index = upper.substring(1).toInt();
        if (index >= 0 && index < MAX_Q_BITS) {
          qBits[index] = false;
        }
      } else if (upper.startsWith("D")) {
        if (upper.indexOf('.') > 0) {
          setBitFromWord(upper, false);
        } else {
          setDWord(upper, 0);
        }
      } else if (upper.startsWith("T")) {
        // Reset timer state (current/remaining and outputs)
        String tName = upper;
        for (int ti = 0; ti < timerCount; ti++) {
          if (toUpper(String(timers[ti].name)) == toUpper(tName)) {
            timers[ti].enabled = false;
            timers[ti].startTime = 0;
            timers[ti].q = false;
            timers[ti].remaining = 0;
          }
        }
      } else if (upper.startsWith("C")) {
        // Reset counter current value to 0 and clear q
        String cName = upper;
        for (int ci = 0; ci < counterCount; ci++) {
          if (String(counters[ci].name) == cName) {
            counters[ci].current = 0;
            counters[ci].q = false;
          }
        }
      }
    }
  }
  
  nodePrevSignalIn[node.id] = signalIn;
}

void executeInstruction(Node& node) {
  bool signalIn = nodeOutputs[node.id];
  String instr = String(node.instruction);
  String args = String(node.args);

  Serial.printf("[DBG %lu] executeInstruction: nodeId=%d addr='%s' instr='%s' args='%s' signalIn=%d\n",
                millis(), node.id, node.addr, instr.c_str(), args.c_str(), signalIn ? 1 : 0);

  // TON - Timer On Delay
  if (instr == "TON") {
    int colonIndex = args.indexOf(':');
    if (colonIndex > 0) {
      String timerName = args.substring(0, colonIndex);
      unsigned long preset = args.substring(colonIndex + 1).toInt();

      Timer* t = nullptr;
      for (int i = 0; i < timerCount; i++) {
        if (toUpper(String(timers[i].name)) == toUpper(timerName)) {
          t = &timers[i];
          break;
        }
      }
      if (!t && timerCount < MAX_TIMERS) {
        t = &timers[timerCount++];
        String tn = toUpper(timerName);
        tn.toCharArray(t->name, sizeof(t->name));
        t->startTime = 0;
        t->preset = preset;
        t->enabled = false;
        t->q = false;
        t->mode = 1; // TON
        t->remaining = 0;
        Serial.printf("[DBG %lu] TON: created timer '%s' preset=%lu (node=%d)\n", millis(), t->name, t->preset, node.id);
      } else if (t) {
        Serial.printf("[DBG %lu] TON: found timer '%s' preset=%lu (node=%d)\n", millis(), t->name, t->preset, node.id);
      }

      if (t) {
        if (signalIn && !t->enabled) {
          // start timer
          t->startTime = millis();
          t->enabled = true;
          t->q = false;
          Serial.printf("[DBG %lu] TON: start timer '%s' at %lu (preset=%lu)\n", millis(), t->name, t->startTime, t->preset);
        } else if (!signalIn) {
          if (t->enabled) {
            Serial.printf("[DBG %lu] TON: cancel timer '%s' (was started at %lu)\n", millis(), t->name, t->startTime);
          }
          t->enabled = false;
          t->q = false;
          t->startTime = 0;
        }

        if (t->enabled) {
          unsigned long elapsed = millis() - t->startTime;
          if (elapsed >= t->preset) {
            if (!t->q) {
              t->q = true;
              Serial.printf("[DBG %lu] TON: timer '%s' elapsed=%lu >= preset=%lu -> q=1\n", millis(), t->name, elapsed, t->preset);
            }
          }
        }

        // reflect timer output to instruction node output immediately
        nodeOutputs[node.id] = t->q;
      }
      return;
    } else {
      Serial.printf("[ERR %lu] TON: invalid args '%s' (expected NAME:MS)\n", millis(), args.c_str());
      nodeOutputs[node.id] = false;
      return;
    }
  }

  // TOFF - Timer Off Delay
  // 입력 true: Q=true (즉시)
  // 입력 false: 지연 후 Q=false
  if (instr == "TOFF") {
    int colonIndex = args.indexOf(':');
    if (colonIndex > 0) {
      String timerName = args.substring(0, colonIndex);
      unsigned long preset = args.substring(colonIndex + 1).toInt();

      Timer* t = nullptr;
      for (int i = 0; i < timerCount; i++) {
        if (toUpper(String(timers[i].name)) == toUpper(timerName)) {
          t = &timers[i];
          break;
        }
      }
      if (!t && timerCount < MAX_TIMERS) {
        t = &timers[timerCount++];
        String tn = toUpper(timerName);
        tn.toCharArray(t->name, sizeof(t->name));
        t->startTime = 0;
        t->preset = preset;
        t->enabled = false;
        t->q = false;  // TOFF Q 초기값은 false (기본 상태)
        t->remaining = 0; // 초기에는 remaining=0 (입력 OFF 상태에서 0으로 보이도록)
        t->mode = 2; // TOFF
        Serial.printf("[DBG %lu] TOFF: created timer '%s' preset=%lu\n", millis(), t->name, t->preset);
      }

      if (t) {
        bool prevSignalIn = nodePrevInputs[node.id];  // 이전 스캔의 input 상태

        if (signalIn) {
          // 입력이 true: TOFF의 경우 입력이 있을 때는 Q=1 유지, remaining은 preset
          t->q = true;
          t->enabled = false;
          t->startTime = 0;
          t->remaining = t->preset;
          Serial.printf("[DBG %lu] TOFF '%s': input=1, hold preset, Q:1 %lu/%lu\n", millis(), t->name, t->remaining, t->preset);
        } else {
          // 입력이 false로 전환(유휴)될 때 카운트다운 시작(발생 엣지: prevSignalIn == true && signalIn == false)
          if (!t->enabled && prevSignalIn) {
            t->startTime = millis();
            t->enabled = true;
            t->q = true; // 카운트다운 동안 Q는 1
            Serial.printf("[DBG %lu] TOFF '%s': falling edge detected, start countdown from %lu ms\n", millis(), t->name, t->preset);
          }

          if (t->enabled) {
            unsigned long elapsed = millis() - t->startTime;
            if (elapsed >= t->preset) {
              t->q = false;
              t->enabled = false;
              t->remaining = 0;
              Serial.printf("[DBG %lu] TOFF '%s': countdown complete, Q:0\n", millis(), t->name);
            } else {
              t->remaining = t->preset - elapsed;
              t->q = true;
              Serial.printf("[DBG %lu] TOFF '%s': counting, Q:1 %lu/%lu\n", millis(), t->name, t->remaining, t->preset);
            }
          } else {
            // 입력이 false이고 카운트다운도 시작되지 않은 경우: Q는 false
            t->q = false;
            t->remaining = 0;
          }
        }

        nodeOutputs[node.id] = t->q;
      }
      return;
    } else {
      Serial.printf("[ERR %lu] TOFF: invalid args '%s' (expected NAME:MS)\n", millis(), args.c_str());
      nodeOutputs[node.id] = false;
      return;
    }
  }

  // TP - Pulse Timer (rising edge generates fixed-duration pulse)
  if (instr == "TP") {
    int colonIndex = args.indexOf(':');
    if (colonIndex > 0) {
      String timerName = args.substring(0, colonIndex);
      unsigned long preset = args.substring(colonIndex + 1).toInt();

      Timer* t = nullptr;
      for (int i = 0; i < timerCount; i++) {
        if (toUpper(String(timers[i].name)) == toUpper(timerName)) {
          t = &timers[i];
          break;
        }
      }
      if (!t && timerCount < MAX_TIMERS) {
        t = &timers[timerCount++];
        String tn = toUpper(timerName);
        tn.toCharArray(t->name, sizeof(t->name));
        t->startTime = 0;
        t->preset = preset;
        t->enabled = false;
        t->q = false;
        t->mode = 3; // TP
        t->remaining = 0;
        Serial.printf("[DBG %lu] TP: created timer '%s' preset=%lu (node=%d)\n", millis(), t->name, t->preset, node.id);
      } else if (t) {
        Serial.printf("[DBG %lu] TP: found timer '%s' preset=%lu (node=%d)\n", millis(), t->name, t->preset, node.id);
      }

      if (t) {
        bool prevSignal = nodePrevInputs[node.id];

        // Start pulse on rising edge only. Once started, do not cancel on input falling;
        // run to completion for the full preset duration.
        if (signalIn && !prevSignal && !t->enabled) {
          t->startTime = millis();
          t->enabled = true;
          t->q = true;
          t->remaining = t->preset;
          Serial.printf("[DBG %lu] TP: pulse start '%s' at %lu (preset=%lu) q=1\n", millis(), t->name, t->startTime, t->preset);
        }

        if (t->enabled) {
          unsigned long elapsed = millis() - t->startTime;
          if (elapsed >= t->preset) {
            t->q = false;
            t->enabled = false;
            t->startTime = 0;
            t->remaining = 0;
            Serial.printf("[DBG %lu] TP: pulse '%s' elapsed=%lu >= preset=%lu -> q=0, disabled\n", millis(), t->name, elapsed, t->preset);
          } else {
            t->remaining = t->preset - elapsed;
            t->q = true;
            Serial.printf("[DBG %lu] TP: pulse '%s' counting, q=1 %lu/%lu\n", millis(), t->name, t->remaining, t->preset);
          }
        } else {
          // not enabled and no pulse running
          t->q = false;
          t->remaining = 0;
        }

        nodeOutputs[node.id] = t->q;
      }
      return;
    } else {
      Serial.printf("[ERR %lu] TP: invalid args '%s' (expected NAME:MS)\n", millis(), args.c_str());
      nodeOutputs[node.id] = false;
      return;
    }
  }

  // CTU - Counter Up
  if (instr == "CTU") {
    int colonIndex = args.indexOf(':');
    if (colonIndex > 0) {
      String counterName = args.substring(0, colonIndex);
      int preset = args.substring(colonIndex + 1).toInt();

      counterName.trim();
      if (counterName.length() == 0) {
        Serial.printf("[ERR %lu] CTU: invalid counter name in args '%s'\n", millis(), args.c_str());
        nodeOutputs[node.id] = false;
        return;
      }

      Counter* c = nullptr;
      for (int i = 0; i < counterCount; i++) {
        if (String(counters[i].name) == counterName) {
          c = &counters[i];
          break;
        }
      }
      if (!c && counterCount < MAX_COUNTERS) {
        c = &counters[counterCount++];
        counterName.toCharArray(c->name, sizeof(c->name));
        c->current = 0;
        c->preset = preset;
        c->q = false;
        Serial.printf("[DBG %lu] CTU: created counter '%s' preset=%d\n", millis(), counterName.c_str(), preset);
      } else if (c) {
        Serial.printf("[DBG %lu] CTU: found counter '%s' current=%d preset=%d\n", millis(), counterName.c_str(), c->current, c->preset);
      }

      if (c) {
        bool prevSignal = nodePrevInputs[node.id];
        if (signalIn && !prevSignal) {
          c->current++;
          Serial.printf("[DBG %lu] CTU: counter '%s' increment -> current=%d\n", millis(), counterName.c_str(), c->current);
          if (c->current >= c->preset) {
            c->q = true;
            Serial.printf("[DBG %lu] CTU: counter '%s' reached preset -> q=1\n", millis(), counterName.c_str());
          }
        }
        // Counter output is available via instruction output; use explicit COIL to link if needed
        nodeOutputs[node.id] = c->q;
      }
      return;
    } else {
      Serial.printf("[ERR %lu] CTU: invalid args '%s' (expected NAME:PRESET)\n", millis(), args.c_str());
      nodeOutputs[node.id] = false;
      return;
    }
  }

  // CTD - Counter Down
  if (instr == "CTD") {
    int colonIndex = args.indexOf(':');
    if (colonIndex > 0) {
      String counterName = args.substring(0, colonIndex);
      int preset = args.substring(colonIndex + 1).toInt();

      counterName.trim();
      if (counterName.length() == 0) {
        Serial.printf("[ERR %lu] CTD: invalid counter name in args '%s'\n", millis(), args.c_str());
        nodeOutputs[node.id] = false;
        return;
      }

      Counter* c = nullptr;
      for (int i = 0; i < counterCount; i++) {
        if (String(counters[i].name) == counterName) {
          c = &counters[i];
          break;
        }
      }
      if (!c && counterCount < MAX_COUNTERS) {
        c = &counters[counterCount++];
        counterName.toCharArray(c->name, sizeof(c->name));
        c->current = preset;
        c->preset = preset;
        c->q = false;
        Serial.printf("[DBG %lu] CTD: created counter '%s' preset=%d (current init=%d)\n", millis(), counterName.c_str(), preset, c->current);
      } else if (c) {
        Serial.printf("[DBG %lu] CTD: found counter '%s' current=%d preset=%d\n", millis(), counterName.c_str(), c->current, c->preset);
      }

      if (c) {
        bool prevSignal = nodePrevInputs[node.id];
        if (signalIn && !prevSignal) {
          c->current--;
          Serial.printf("[DBG %lu] CTD: counter '%s' decrement -> current=%d\n", millis(), counterName.c_str(), c->current);
          if (c->current <= 0) {
            c->q = true;
            Serial.printf("[DBG %lu] CTD: counter '%s' reached 0 -> q=1\n", millis(), counterName.c_str());
          }
        }
        // Counter output is available via instruction output; use explicit COIL to link if needed
        nodeOutputs[node.id] = c->q;
      }
      return;
    } else {
      Serial.printf("[ERR %lu] CTD: invalid args '%s' (expected NAME:PRESET)\n", millis(), args.c_str());
      nodeOutputs[node.id] = false;
      return;
    }
  }

  // Other instructions: arithmetic, logic, comparisons, move, etc.
  // Execute only if signalIn (i.e., rung condition true)
  if (signalIn) {
    if (instr == "ADD") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        setDWord(dest, val1 + val2);
        Serial.printf("[DBG %lu] ADD: %s = %u + %u -> %u\n", millis(), dest.c_str(), val1, val2, getDWord(dest));
      }
    } else if (instr == "SUB") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        setDWord(dest, val1 - val2);
        Serial.printf("[DBG %lu] SUB: %s = %u - %u -> %u\n", millis(), dest.c_str(), val1, val2, getDWord(dest));
      }
    } else if (instr == "MUL") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        setDWord(dest, val1 * val2);
        Serial.printf("[DBG %lu] MUL: %s = %u * %u -> %u\n", millis(), dest.c_str(), val1, val2, getDWord(dest));
      }
    } else if (instr == "DIV") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        if (val2 != 0) setDWord(dest, val1 / val2);
        Serial.printf("[DBG %lu] DIV: %s = %u / %u -> %u\n", millis(), dest.c_str(), val1, val2, getDWord(dest));
      }
    } else if (instr == "MOD") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        if (val2 != 0) setDWord(dest, val1 % val2);
        Serial.printf("[DBG %lu] MOD: %s = %u %% %u -> %u\n", millis(), dest.c_str(), val1, val2, getDWord(dest));
      }
    } else if (instr == "SHL" || instr == "SHR" || instr == "ROL" || instr == "ROR") {
      int colon = args.indexOf(':');
      if (colon > 0) {
        String dest = args.substring(0, colon);
        String src = args.substring(colon + 1);
        uint32_t val = getDWord(src);
        int shift = getDWord(dest);
        uint32_t res = val;
        if (instr == "SHL") res = val << shift;
        else if (instr == "SHR") res = val >> shift;
        else if (instr == "ROL") res = (val << shift) | (val >> (32 - shift));
        else if (instr == "ROR") res = (val >> shift) | (val << (32 - shift));
        setDWord(dest, res);
        Serial.printf("[DBG %lu] %s: %s = %u (rot/shift %d)\n", millis(), instr.c_str(), dest.c_str(), getDWord(dest), shift);
      }
    } else if (instr == "AND" || instr == "OR" || instr == "XOR") {
      int colon1 = args.indexOf(':');
      int colon2 = args.indexOf(':', colon1 + 1);
      if (colon1 > 0 && colon2 > colon1) {
        String dest = args.substring(0, colon1);
        String op1 = args.substring(colon1 + 1, colon2);
        String op2 = args.substring(colon2 + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        uint32_t res = val1;
        if (instr == "AND") res = val1 & val2;
        else if (instr == "OR") res = val1 | val2;
        else if (instr == "XOR") res = val1 ^ val2;
        setDWord(dest, res);
        Serial.printf("[DBG %lu] %s: %s = %u\n", millis(), instr.c_str(), dest.c_str(), getDWord(dest));
      }
    } else if (instr == "NOT") {
      int colon = args.indexOf(':');
      if (colon > 0) {
        String dest = args.substring(0, colon);
        String src = args.substring(colon + 1);
        uint32_t val = getDWord(src);
        setDWord(dest, ~val);
        Serial.printf("[DBG %lu] NOT: %s = ~%u -> %u\n", millis(), dest.c_str(), val, getDWord(dest));
      }
    } else if (instr == "EQ" || instr == "GT" || instr == "GE" || instr == "LT" || instr == "LE" || instr == "NE") {
      int colon = args.indexOf(':');
      if (colon > 0) {
        String op1 = args.substring(0, colon);
        String op2 = args.substring(colon + 1);
        uint32_t val1 = getDWord(op1);
        uint32_t val2 = getDWord(op2);
        bool res = false;
        if (instr == "EQ") res = (val1 == val2);
        else if (instr == "GT") res = (val1 > val2);
        else if (instr == "GE") res = (val1 >= val2);
        else if (instr == "LT") res = (val1 < val2);
        else if (instr == "LE") res = (val1 <= val2);
        else if (instr == "NE") res = (val1 != val2);
        nodeOutputs[node.id] = res;
        Serial.printf("[DBG %lu] %s: %s (%u) vs %s (%u) -> %d\n", millis(), instr.c_str(), op1.c_str(), val1, op2.c_str(), val2, res ? 1 : 0);
      }
    } else if (instr == "MOVE") {
      int colon = args.indexOf(':');
      if (colon > 0) {
        String dest = args.substring(0, colon);
        String src = args.substring(colon + 1);
        uint32_t val = getDWord(src);
        setDWord(dest, val);
        Serial.printf("[DBG %lu] MOVE: %s = %u (from %s)\n", millis(), dest.c_str(), val, src.c_str());
      }
    } else {
      Serial.printf("[DBG %lu] Instruction '%s' executed (signalIn=1) args='%s'\n", millis(), instr.c_str(), args.c_str());
    }
  } else {
    // If not signalIn and instruction is comparison that sets nodeOutputs earlier, keep that behavior.
    // Otherwise leave nodeOutputs[node.id] as previously set by placeholder in executeScan.
    // Debug skip
    // Serial.printf("[DBG %lu] Instruction '%s' skipped (signalIn=0)\n", millis(), instr.c_str());
  }
}

void executeScan() {
  // Store previous outputs
  memcpy(nodePrevOutputs, nodeOutputs, sizeof(nodeOutputs));
  // Store previous inputs for edge detection
  memcpy(nodePrevInputs, nodeInputs, sizeof(nodeInputs));

  // Initialize nodeInputs and nodeOutputs
  memset(nodeInputs, 0, sizeof(nodeInputs));
  memset(nodeOutputs, 0, sizeof(nodeOutputs));

  // Network nodes have input = true
  for (int i = 0; i < nodeCount; i++) {
    if (nodes[i].type == NODE_NETWORK) {
      nodeInputs[nodes[i].id] = true;
    }
  }

  // Get all unique networkIds (network addresses) and collect them
  char networkIds[MAX_NODES][16];
  int networkIdCount = 0;
  for (int i = 0; i < nodeCount; i++) {
    if (strlen(nodes[i].networkId) > 0) {
      bool found = false;
      for (int j = 0; j < networkIdCount; j++) {
        if (strcmp(networkIds[j], nodes[i].networkId) == 0) {
          found = true;
          break;
        }
      }
      if (!found && networkIdCount < MAX_NODES) {
        strcpy(networkIds[networkIdCount++], nodes[i].networkId);
      }
    }
  }

  // Sort networkIds lexicographically (so N0 < N1 < N2, etc.)
  for (int i = 0; i < networkIdCount - 1; i++) {
    for (int j = i + 1; j < networkIdCount; j++) {
      if (strcmp(networkIds[i], networkIds[j]) > 0) {
        char temp[16];
        strcpy(temp, networkIds[i]);
        strcpy(networkIds[i], networkIds[j]);
        strcpy(networkIds[j], temp);
      }
    }
  }

  // Execute nodes by network order
  for (int n = 0; n < networkIdCount; n++) {
    const char* currentNetworkId = networkIds[n];

    // 1) First pass: evaluate NETWORK nodes and propagate their outputs immediately
    for (int i = 0; i < nodeCount; i++) {
      if (strcmp(nodes[i].networkId, currentNetworkId) != 0) continue;
      if (nodes[i].type != NODE_NETWORK) continue;

      int id = nodes[i].id;
      nodeOutputs[id] = evaluateNode(nodes[i]);

      // propagate outputs to connected nodes (OR logic for parallel branches)
      for (int j = 0; j < linkCount; j++) {
        if (links[j].fromNode == id) {
          nodeInputs[links[j].toNode] = nodeInputs[links[j].toNode] || nodeOutputs[id];
        }
      }
    }

    // 2) Second pass: evaluate ALL non-NETWORK nodes to compute nodeOutputs
    //    For instruction nodes we do NOT execute them here; instead we set their nodeOutputs
    //    as the rung condition (nodeInputs[id]) so executeInstruction() can read correct signalIn later.
    //    Process nodes multiple times until no changes occur (topological sort for cyclic dependencies)
    bool changed = true;
    int iterations = 0;
    const int MAX_ITERATIONS = 10;  // Prevent infinite loops
    
    while (changed && iterations < MAX_ITERATIONS) {
      changed = false;
      iterations++;
      
      for (int i = 0; i < nodeCount; i++) {
        if (strcmp(nodes[i].networkId, currentNetworkId) != 0) continue;
        if (nodes[i].type == NODE_NETWORK) continue;

        int id = nodes[i].id;
        bool prevOutput = nodeOutputs[id];

        if (nodes[i].type == NODE_INSTRUCTION) {
          // Instruction nodes: placeholder = rung condition; actual effect handled in final pass
          nodeOutputs[id] = nodeInputs[id];
        } else {
          // Other nodes: evaluate normally (contacts, coils (logical output), edges, invert, set/reset)
          nodeOutputs[id] = evaluateNode(nodes[i]);
        }

        // Detect change
        if (nodeOutputs[id] != prevOutput) {
          changed = true;
        }

        // propagate outputs to connected nodes for non-instruction nodes only (OR logic for parallel branches)
        if (nodes[i].type != NODE_INSTRUCTION) {
          for (int j = 0; j < linkCount; j++) {
            if (links[j].fromNode == id) {
              nodeInputs[links[j].toNode] = nodeInputs[links[j].toNode] || nodeOutputs[id];
            }
          }
        }
      }
    }
    
    if (iterations >= MAX_ITERATIONS) {
      Serial.printf("[WARN] Network %s exceeded max iterations (%d)\n", currentNetworkId, MAX_ITERATIONS);
    }
  }

  // 3) Final pass: execute coils and instructions (timers/counters are created/updated here)
  for (int n = 0; n < networkIdCount; n++) {
    const char* currentNetworkId = networkIds[n];
    for (int i = 0; i < nodeCount; i++) {
      if (strcmp(nodes[i].networkId, currentNetworkId) != 0) continue;

      if (nodes[i].type == NODE_COIL || nodes[i].type == NODE_SET || nodes[i].type == NODE_RESET) {
        executeCoil(nodes[i]);
      } else if (nodes[i].type == NODE_INSTRUCTION) {
        executeInstruction(nodes[i]);
      }
    }
  }
}

// ============ PERSISTENCE FUNCTIONS ============
// store only RUN/STOP state for persistence
void savePlcStatusToPrefs(PLCStatus s) {
  if (s == RUN || s == STOP) {
    prefs.putUInt("plc_status", (unsigned int)s);
    Serial.printf("[DBG %lu] savePlcStatusToPrefs: saved %d\n", millis(), (int)s);
  }
}

PLCStatus loadPlcStatusFromPrefs() {
  unsigned int v = prefs.getUInt("plc_status", (unsigned int)STOP);
  if (v <= RESET) return (PLCStatus)v;
  return STOP;
}

// Centralized setter to update status, LED and broadcast; optionally persist
void setPlcStatus(PLCStatus s, bool persist=true) {
  plcStatus = s;
  if (s == STOP) {
    // STOP 상태일 때 모든 M 비트(내부 메모리) 초기화
    memset(mBits, 0, sizeof(mBits));
    Serial.println("[DBG] STOP: M bits (internal memory) cleared");

    // STOP 상태일 때 모든 Q(출력) 비트 초기화
    memset(qBits, 0, sizeof(qBits));
    Serial.println("[DBG] STOP: Q outputs cleared");

    // Stop timers (do not remove presets) and reset their startTime so current stops reporting
    for (int i = 0; i < timerCount; i++) {
      timers[i].enabled = false;
      timers[i].q = false;
      timers[i].startTime = 0;
    }

    // reset counters current (stop counting)
    for (int i = 0; i < counterCount; i++) {
      counters[i].current = 0;
      counters[i].q = false;
    }

    Serial.printf("[DBG %lu] STOP: timers and counters reset\n", millis());
  }
  // LED 업데이트를 loop()에서 처리하도록 플래그 설정
  ledUpdateNeeded = true;
  sendPlcStatus(true);
  if (persist) savePlcStatusToPrefs(s);
  Serial.printf("[DBG %lu] setPlcStatus: %d (persist=%d)\n", millis(), (int)s, persist ? 1 : 0);
}

// Convenience to set error and broadcast log (keeps ERROR non-persistent)
void setPlcError(const String &msg) {
  plcErrorMsg = msg;
  // Use setPlcStatus so LED and broadcast update consistently; do not persist ERROR
  setPlcStatus(ERROR, false);
  Serial.printf("[ERR %lu] setPlcError: %s\n", millis(), msg.c_str());
}

// ============ LOGGING FUNCTIONS ============
// Logging broadcast (device -> clients)
void broadcastLog(const String &type, const String &msg) {
  DynamicJsonDocument d(384);
  JsonObject l = d.createNestedObject("log");
  l["type"] = type;
  l["msg"] = msg;
  l["ts"] = millis();
  String out;
  serializeJson(d, out);
  ws.textAll(out);
  Serial.printf("[LOG %lu] %s: %s\n", millis(), type.c_str(), msg.c_str());
}

// ============ LED FUNCTIONS ============
void updatePlcLed() {
  CRGB c = CRGB::Black;
  switch (plcStatus) {
    case RUN: c = CRGB::Red; break;
    case STOP: c = CRGB::Green; break;
    case ERROR: c = (millis() % 500 < 250 ? CRGB::Orange : CRGB::Black); break;
    case RESET: c = CRGB::Blue; break;
  }
  fill_solid(plc_leds, PLC_NUM_LEDS, c);
  FastLED.show();
}

// ============ WEBSOCKET FUNCTIONS ============
void sendPlcStatus(bool force) {
  unsigned long now = millis();
  if (!force && (now - lastStatusSent) < statusIntervalMs) {
    return;
  }
  lastStatusSent = now;

  DynamicJsonDocument d(4096);
  d["status"] = plcStatus == RUN ? "RUN" : plcStatus == STOP ? "STOP" :
                plcStatus == ERROR ? "ERROR" : "RESET";

  if (plcStatus == ERROR && !plcErrorMsg.isEmpty()) {
    d["error"] = plcErrorMsg;
  }

  JsonObject mem = d.createNestedObject("memory");

  // M 비트들
  JsonObject mObj = mem.createNestedObject("m");
  for (int i = 0; i < MAX_M_BITS; i++) {
    if (mBits[i]) {
      char key[8];
      sprintf(key, "M%d", i);
      mObj[key] = true;
    }
  }

  // D 워드들
  JsonObject dObj = mem.createNestedObject("d");
  for (int i = 0; i < MAX_D_WORDS; i++) {
    if (dWords[i] != 0) {
      char key[8];
      sprintf(key, "D%d", i);
      dObj[key] = dWords[i];
    }
  }

  // I 입력
  JsonObject iObj = mem.createNestedObject("i");
  for (int i = 0; i < MAX_I_BITS; i++) {
    if (iBits[i]) {
      char key[8];
      sprintf(key, "I%d", i);
      iObj[key] = true;
    }
  }

  // Q 출력
  JsonObject qObj = mem.createNestedObject("q");
  for (int i = 0; i < MAX_Q_BITS; i++) {
    if (qBits[i]) {
      char key[8];
      sprintf(key, "Q%d", i);
      qObj[key] = true;
    }
  }

    // T 타이머
  JsonObject tObj = mem.createNestedObject("t");
  for (int i = 0; i < timerCount; i++) {
    JsonObject t = tObj.createNestedObject(timers[i].name);
    t["q"] = timers[i].q;
    // Report timers: for TOFF and TP report remaining; otherwise report elapsed when enabled
    if (timers[i].mode == 2 || timers[i].mode == 3) {
      // TOFF/TP: report remaining (preset - elapsed while enabled), or preset when held
      t["current"] = timers[i].remaining;
    } else {
      if (timers[i].enabled) {
        unsigned long elapsed = millis() - timers[i].startTime;
        uint32_t current = (uint32_t)(elapsed >= timers[i].preset ? timers[i].preset : elapsed);
        t["current"] = current;
      } else {
        t["current"] = 0;
      }
    }
    t["preset"] = timers[i].preset;
    t["enabled"] = timers[i].enabled;
  }

  // C 카운터
  JsonObject cObj = mem.createNestedObject("c");
  for (int i = 0; i < counterCount; i++) {
    JsonObject c = cObj.createNestedObject(counters[i].name);
    c["q"] = counters[i].q;
    c["current"] = counters[i].current;
    c["preset"] = counters[i].preset;
  }

  String out;
  serializeJson(d, out);
  ws.textAll(out);
}

void handleLoad() {
  // 로직 로드 전에 networkId 재할당 (만약 -1이거나 비어있으면)
  if (logicDoc.containsKey("nodes") && logicDoc.containsKey("linkData")) {
    JsonArray nodesArray = logicDoc["nodes"];
    JsonArray linksArray = logicDoc["linkData"];
    bool needsAssignment = false;
    
    for (JsonObject nodeObj : nodesArray) {
      String netIdStr = nodeObj["networkId"].as<String>();
      if (netIdStr == "" || netIdStr == "-1") {
        needsAssignment = true;
        break;
      }
    }
    
    if (needsAssignment) {
      assignNetworkAddresses(nodesArray, linksArray);
    }
  }

  // Ensure networkId values are stored in consistent string format before sending to clients
  if (logicDoc.containsKey("nodes")) {
    coerceNetworkIdsToString(logicDoc["nodes"].as<JsonArray>());
  }
  
  String output;
  serializeJson(logicDoc, output);
  ws.textAll(output);
  Serial.println("Logic sent to clients");
}

// ============ NETWORK ADDRESS ASSIGNMENT ============
// 네트워크 주소 자동 할당 함수
void assignNetworkAddresses(JsonArray nodesArray, JsonArray linksArray) {
  // nodeId -> networkId 매핑
  char networkMapping[MAX_NODES][16];
  memset(networkMapping, 0, sizeof(networkMapping));
  
  int networkIndex = 0;
  
  // Step 1: NETWORK 노드를 찾아서 그에 연결된 노드들 그룹화
  for (JsonObject nodeObj : nodesArray) {
    int nodeId = nodeObj["id"];
    String typeStr = nodeObj["type"];
    typeStr.toUpperCase();
    
    if (typeStr.indexOf("NETWORK") >= 0) {
      // NETWORK 노드 발견
      String networkAddr = nodeObj["addr"];
      char networkId[8];
      
      // networkId 생성
      if (networkAddr == "" || networkAddr == "N" || networkAddr.toInt() < 0) {
        sprintf(networkId, "N%d", networkIndex++);
      } else {
        networkAddr.toCharArray(networkId, sizeof(networkId));
      }
      
      // 이 NETWORK 노드에 연결된 모든 노드들 찾기
      strcpy(networkMapping[nodeId], networkId);
      
      // BFS로 연결된 모든 노드 찾기
      for (JsonObject linkObj : linksArray) {
        if ((int)linkObj["from"] == nodeId) {
          int toNode = linkObj["to"];
          strcpy(networkMapping[toNode], networkId);
        }
      }
    }
  }
  
  // Step 2: 아직 네트워크가 할당되지 않은 노드들 처리
  for (JsonObject nodeObj : nodesArray) {
    int nodeId = nodeObj["id"];
    if (strlen(networkMapping[nodeId]) == 0) {
      char buf[8];
      sprintf(buf, "N%d", networkIndex++);
      strcpy(networkMapping[nodeId], buf);
    }
  }
  
  // Step 3: JSON 문서 업데이트
  int idx = 0;
  for (JsonObject nodeObj : nodesArray) {
    int nodeId = nodeObj["id"];
    nodeObj["networkId"] = networkMapping[nodeId];
    idx++;
  }
  
  // 디버그 메시지
  Serial.println("[DBG] Network addresses assigned:");
  for (JsonObject nodeObj : nodesArray) {
    Serial.printf("  Node %d: %s\n", (int)nodeObj["id"], nodeObj["networkId"].as<String>().c_str());
  }
}

// ============ NORMALIZE / COERCE HELPERS ============
// Ensure nodes[].networkId stored as a canonical string: "N<number>" or "-1" or user-provided string (uppercased)
void coerceNetworkIdsToString(JsonArray nodesArray) {
  for (JsonObject nodeObj : nodesArray) {
    String nid = nodeObj["networkId"].as<String>();
    nid.trim();
    if (nid.length() == 0) {
      nodeObj["networkId"] = String("-1");
      continue;
    }
    if (nid == "-1") {
      nodeObj["networkId"] = String("-1");
      continue;
    }
    // If already starts with N, normalize to uppercase "Nxx"
    if (nid.charAt(0) == 'N' || nid.charAt(0) == 'n') {
      nid.toUpperCase();
      nodeObj["networkId"] = nid;
      continue;
    }
    // If numeric string, convert to "N<number>" (non-negative => N#; negative => "-1")
    bool allDigits = true;
    int len = nid.length();
    for (int i = 0; i < len; ++i) {
      char c = nid.charAt(i);
      if (!isDigit(c) && !(i == 0 && c == '-')) {
        allDigits = false;
        break;
      }
    }
    if (allDigits) {
      int v = nid.toInt();
      if (v >= 0) {
        char buf[12];
        sprintf(buf, "N%d", v);
        nodeObj["networkId"] = String(buf);
      } else {
        nodeObj["networkId"] = String("-1");
      }
      continue;
    }
    // Otherwise keep string but uppercase for consistency
    nid.toUpperCase();
    nodeObj["networkId"] = nid;
  }
}

// Sync timers and counters from loaded logic (call after logicDoc is set)
void syncTimersAndCountersFromLogic() {
  if (!logicDoc.containsKey("nodes")) return;
  JsonArray nodesArray = logicDoc["nodes"];

  // Update timers
  for (JsonObject nodeObj : nodesArray) {
    String typeStr = nodeObj["type"] | "";
    typeStr.toUpperCase();
    if (typeStr.indexOf("INSTRUCTION") < 0) continue;

    String instr = nodeObj["instruction"] | "";
    instr.toUpperCase();
    String args = nodeObj["args"] | "";

    if (instr == "TON" || instr == "TOFF" || instr == "TP") {
      int colonIndex = args.indexOf(':');
      if (colonIndex <= 0) continue;
      String timerName = args.substring(0, colonIndex);
      unsigned long preset = (unsigned long)args.substring(colonIndex + 1).toInt();
      if (timerName.length() == 0) continue;

      // find existing timer (case-insensitive)
      Timer* t = nullptr;
      for (int i = 0; i < timerCount; i++) {
        if (toUpper(String(timers[i].name)) == toUpper(timerName)) {
          t = &timers[i];
          break;
        }
      }
      if (t) {
        // update preset only; keep enabled/startTime to preserve running behavior
        t->preset = preset;
        if (instr == "TON") t->mode = 1;
        else if (instr == "TOFF") t->mode = 2;
        else if (instr == "TP") t->mode = 3;
        Serial.printf("[DBG %lu] syncTimers: updated timer '%s' preset=%lu\n", millis(), t->name, t->preset);
      } else {
        // create if not exist (disabled, startTime=0)
        if (timerCount < MAX_TIMERS) {
          t = &timers[timerCount++];
          String tn = toUpper(timerName);
          tn.toCharArray(t->name, sizeof(t->name));
          t->preset = preset;
          t->startTime = 0;
          t->enabled = false;
          t->q = false;
          t->remaining = preset;
          if (instr == "TON") t->mode = 1;
          else if (instr == "TOFF") t->mode = 2;
          else if (instr == "TP") t->mode = 3;
          Serial.printf("[DBG %lu] syncTimers: created timer '%s' preset=%lu\n", millis(), t->name, t->preset);
        }
      }
    } else if (instr == "CTU" || instr == "CTD") {
      int colonIndex = args.indexOf(':');
      if (colonIndex <= 0) continue;
      String counterName = args.substring(0, colonIndex);
      int preset = args.substring(colonIndex + 1).toInt();
      if (counterName.length() == 0) continue;

      Counter* c = nullptr;
      for (int i = 0; i < counterCount; i++) {
        if (String(counters[i].name) == counterName) {
          c = &counters[i];
          break;
        }
      }
      if (c) {
        c->preset = preset;
        Serial.printf("[DBG %lu] syncCounters: updated counter '%s' preset=%d\n", millis(), c->name, c->preset);
      } else {
        if (counterCount < MAX_COUNTERS) {
          c = &counters[counterCount++];
          counterName.toCharArray(c->name, sizeof(c->name));
          c->preset = preset;
          // For CTD (counter down) initialize current to preset so it counts down from preset -> 0
          if (instr == "CTD") c->current = preset; else c->current = 0;
          c->q = false;
          Serial.printf("[DBG %lu] syncCounters: created counter '%s' preset=%d current=%d\n", millis(), c->name, c->preset, c->current);
        }
      }
    }
  }
}

void handleSave(String jsonStr) {
  DynamicJsonDocument doc(8192);
  DeserializationError error = deserializeJson(doc, jsonStr);

  if (error) {
    Serial.print("JSON Error: ");
    Serial.println(error.c_str());
    return;
  }

  // 네트워크 주소 자동 할당 (만약 비어있거나 -1이면)
  if (doc.containsKey("nodes") && doc.containsKey("linkData")) {
    JsonArray nodesArray = doc["nodes"];
    JsonArray linksArray = doc["linkData"];
    bool needsAssignment = false;
    
    for (JsonObject nodeObj : nodesArray) {
      String netIdStr = nodeObj["networkId"].as<String>();
      if (netIdStr == "" || netIdStr == "-1") {
        needsAssignment = true;
        break;
      }
    }
    
    if (needsAssignment) {
      assignNetworkAddresses(nodesArray, linksArray);
    }
  }

  // Clear old logic
  nodeCount = 0;
  linkCount = 0;
  memset(nodeOutputs, 0, sizeof(nodeOutputs));
  memset(nodePrevOutputs, 0, sizeof(nodePrevOutputs));

  // Load nodes
  if (doc.containsKey("nodes")) {
    JsonArray nodesArray = doc["nodes"];
    int i = 0;
    for (JsonObject nodeObj : nodesArray) {
      if (i >= MAX_NODES) break;

      nodes[i].id = nodeObj["id"];
      
      // Load networkId as string (e.g., "N0", "N1")
      String netIdStr = nodeObj["networkId"];
      memset(nodes[i].networkId, 0, sizeof(nodes[i].networkId));
      netIdStr.toCharArray(nodes[i].networkId, sizeof(nodes[i].networkId));
      
      nodes[i].x = nodeObj["x"];
      nodes[i].y = nodeObj["y"];

      String typeStr = nodeObj["type"];
      typeStr.toUpperCase();

      if (typeStr.indexOf("OPEN") >= 0) nodes[i].type = NODE_OPEN;
      else if (typeStr.indexOf("CLOSE") >= 0) nodes[i].type = NODE_CLOSE;
      else if (typeStr.indexOf("RISING") >= 0) nodes[i].type = NODE_RISING;
      else if (typeStr.indexOf("FALLING") >= 0) nodes[i].type = NODE_FALLING;
      else if (typeStr.indexOf("INVERT") >= 0) nodes[i].type = NODE_INVERT;
      else if (typeStr.indexOf("COIL") >= 0) nodes[i].type = NODE_COIL;
      // Check for RESET before SET because "RESET" contains "SET"
      else if (typeStr.indexOf("RESET") >= 0) nodes[i].type = NODE_RESET;
      else if (typeStr.indexOf("SET") >= 0) nodes[i].type = NODE_SET;
      else if (typeStr.indexOf("INSTRUCTION") >= 0) nodes[i].type = NODE_INSTRUCTION;
      else if (typeStr.indexOf("NETWORK") >= 0) nodes[i].type = NODE_NETWORK;
      else nodes[i].type = NODE_OPEN;

      String addr = nodeObj["addr"];
      String tag = nodeObj["tag"];
      String instruction = nodeObj["instruction"];
      String args = nodeObj["args"];

      addr.toCharArray(nodes[i].addr, sizeof(nodes[i].addr));
      tag.toCharArray(nodes[i].tag, sizeof(nodes[i].tag));
      instruction.toCharArray(nodes[i].instruction, sizeof(nodes[i].instruction));
      args.toCharArray(nodes[i].args, sizeof(nodes[i].args));

      nodes[i].setpoint = nodeObj["setpoint"] | 0;

      nodeOutputs[nodes[i].id] = false;
      nodePrevOutputs[nodes[i].id] = false;
      i++;
    }
    nodeCount = i;
  }

  // Load links
  if (doc.containsKey("linkData")) {
    JsonArray linksArray = doc["linkData"];
    int i = 0;
    for (JsonObject linkObj : linksArray) {
      if (i >= MAX_LINKS) break;

      links[i].fromNode = linkObj["from"];
      links[i].toNode = linkObj["to"];

      String fromPort = linkObj["fromPort"];
      String toPort = linkObj["toPort"];

      fromPort.toCharArray(links[i].fromPort, sizeof(links[i].fromPort));
      toPort.toCharArray(links[i].toPort, sizeof(links[i].toPort));

      i++;
    }
    linkCount = i;
  }

  logicDoc = doc;

  // Coerce networkId values to canonical string form before saving
  if (doc.containsKey("nodes")) {
    coerceNetworkIdsToString(doc["nodes"].as<JsonArray>());
  }

  // Save to LittleFS
  File file = LittleFS.open("/logic.json", "w");
  if (file) {
    serializeJson(doc, file);
    file.close();
    Serial.println("Logic saved to storage");
  }
}

// WebSocket 이벤트 핸들러
void onWsEvent(AsyncWebSocket * server, AsyncWebSocketClient * client, AwsEventType type, void * arg, uint8_t *data, size_t len) {
  if(type == WS_EVT_CONNECT){
    Serial.printf("[DBG %lu] WS_EVT_CONNECT: client=%u\n", millis(), client ? client->id() : 0);
    sendPlcStatus(true); // immediate on new client
  } else if(type == WS_EVT_DISCONNECT){
    Serial.printf("[DBG %lu] WS_EVT_DISCONNECT: client=%u\n", millis(), client ? client->id() : 0);
  } else if(type == WS_EVT_DATA){
    AwsFrameInfo * info = (AwsFrameInfo*)arg;
    // Only process final text frames
    if (info->final && info->opcode == WS_TEXT && len < 1024) { // 메시지 크기 제한 1KB
      String msg = ""; for(size_t i=0;i<len;i++) msg += (char)data[i];
      msg.trim();
      Serial.printf("[DBG %lu] WS_EVT_DATA from client=%u: '%s'\n", millis(), client ? client->id() : 0, msg.c_str());

      if(msg == "RUN") { setPlcStatus(RUN, true); Serial.println("[DBG] PLC status -> RUN"); }
      else if(msg == "STOP") { setPlcStatus(STOP, true); Serial.println("[DBG] PLC status -> STOP"); }
      else if(msg == "RESET") {
        resetMemory();
        setPlcStatus(RESET, false);
        Serial.println("[DBG] PLC RESET received: memory cleared");
      } else if (msg.startsWith("SET ")) {
        int space1 = msg.indexOf(' ', 4);
        if (space1 > 4) {
          String addr = msg.substring(4, space1);
          addr.toUpperCase();
          int val = msg.substring(space1 + 1).toInt();
          
          if (addr.startsWith("M")) {
            setMBit(addr, val ? 1 : 0);
          } else if (addr.startsWith("Q")) {
            setQBit(addr, val ? 1 : 0);
          } else if (addr.startsWith("I")) {
            setIBit(addr, val ? 1 : 0);
          } else if (addr.startsWith("D")) {
            setDWord(addr, val);
          }
          Serial.printf("[DBG] SET %s = %d\n", addr.c_str(), val);
        }
      } else if (msg.startsWith("{")) {
        handleSave(msg);
        ws.textAll("{\"status\":\"saved\"}");
        broadcastLog("info", "Logic deployed successfully");
      } else if (msg == "/load") {
        handleLoad();
      } else {
        Serial.printf("[DBG %lu] WS unknown command: %s\n", millis(), msg.c_str());
      }
      // Force immediate broadcast so clients see state change promptly
      sendPlcStatus(true);
    } else {
      // ignore fragments / binary for now
    }
  } else {
    Serial.printf("[DBG %lu] WS other event type=%d\n", millis(), (int)type);
  }
}

// ============ HTTP ENDPOINTS ============
// HTTP 엔드포인트 설정
void setupHttpEndpoints() {
  // 기본 웹 UI 제공
  server.on("/", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(LittleFS, "/index.html", "text/html");
  });

  // 기타 정적 파일들
  server.on("/style.css", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(LittleFS, "/style.css", "text/css");
  });

  server.on("/script.js", HTTP_GET, [](AsyncWebServerRequest *request){
    request->send(LittleFS, "/script.js", "application/javascript");
  });

  // 로직 로드 요청
  server.on("/load", HTTP_GET, [](AsyncWebServerRequest *request){
    if (LittleFS.exists("/logic.json")) {
      request->send(LittleFS, "/logic.json", "application/json");
    } else {
      request->send(404, "text/plain", "Logic file not found");
    }
  });

  // Trigger auto-connect using saved credentials (if any)
  server.on("/wifi/autoconnect", HTTP_GET, [](AsyncWebServerRequest *request){
    String savedSsid = prefs.getString("sta_ssid", "");
    String savedPass = prefs.getString("sta_pass", "");
    DynamicJsonDocument doc(256);
    if (savedSsid.length() == 0) {
      doc["success"] = false;
      doc["error"] = "No saved credentials";
      String resp; serializeJson(doc, resp);
      request->send(400, "application/json", resp);
      return;
    }
    // parse optional parameters: interval (ms) and attempts (count)
    unsigned long intervalMs = 5000; // default 5 seconds
    int attempts = 3; // default 3 attempts
    if (request->hasParam("interval")) {
      String v = request->getParam("interval")->value();
      unsigned long t = (unsigned long) v.toInt();
      if (t > 0) intervalMs = t;
    }
    if (request->hasParam("attempts")) {
      String v = request->getParam("attempts")->value();
      int a = v.toInt();
      if (a > 0) attempts = a;
    }

    if (WiFi.getMode() != WIFI_AP_STA) {
      WiFi.mode(WIFI_AP_STA);
      delay(200);
    }

    bool connected = false;
    for (int attempt = 1; attempt <= attempts; ++attempt) {
      Serial.printf("[DBG] AutoConnect attempt %d/%d using saved SSID=%s\n", attempt, attempts, savedSsid.c_str());
      WiFi.begin(savedSsid.c_str(), savedPass.c_str());
      unsigned long start = millis();
      while (WiFi.status() != WL_CONNECTED && (millis() - start) < intervalMs) {
        delay(200);
      }
      if (WiFi.status() == WL_CONNECTED) {
        connected = true;
        break;
      }
      // optional small pause before next attempt
      delay(200);
    }

    if (connected && WiFi.status() == WL_CONNECTED) {
      staConnected = true;
      staIP = WiFi.localIP();
      doc["success"] = true;
      doc["ip"] = staIP.toString();
      String resp; serializeJson(doc, resp);
      request->send(200, "application/json", resp);
      broadcastLog("system", String("Auto WiFi Connected: ") + staIP.toString());
    } else {
      staConnected = false;
      doc["success"] = false;
      doc["error"] = "Auto connect failed or timed out";
      doc["attempts"] = attempts;
      doc["interval_ms"] = intervalMs;
      String resp; serializeJson(doc, resp);
      request->send(200, "application/json", resp);
      broadcastLog("system", "Auto WiFi connect failed or timed out");
    }
  });

  // Verify 요청 (저장된 로직 확인용)
  server.on("/verify", HTTP_GET, [](AsyncWebServerRequest *request){
    if (LittleFS.exists("/logic.json")) {
      request->send(LittleFS, "/logic.json", "application/json");
    } else {
      request->send(404, "text/plain", "Logic file not found");
    }
  });

  // 로직 저장 요청 (Deploy)
  server.on("/save", HTTP_POST, [](AsyncWebServerRequest *request){
    Serial.printf("[DBG %lu] /save: request received\n", millis());

    // capture previous PLC state so we can restore it after deploying
    PLCStatus prevStatus = plcStatus;

    // Optionally pause (you can omit stopping entirely) - here we stop temporarily but will restore prevStatus
    setPlcStatus(STOP, false); // keep non-persistent during deploy

    // Clear runtime memory (M/D/Q/timers/counters) before loading new logic
    // This ensures memory monitor shows only currently used addresses after deploy
    resetMemory();

    // JSON 데이터 수신
    if (request->hasParam("logic", true)) {
      String jsonData = request->getParam("logic", true)->value();
      Serial.printf("[DBG %lu] /save: JSON data received, len=%d\n", millis(), jsonData.length());

      // 파일에 저장
      File file = LittleFS.open("/logic.json", "w");
      if (file) {
        file.print(jsonData);
        file.close();
        Serial.printf("[DBG %lu] /save: file written successfully\n", millis());

        // 로직 로드 (this sets logicDoc and populates nodes/links arrays)
        handleSave(jsonData);

        // sync timers/counters from saved logic immediately so presets apply without full reset
        syncTimersAndCountersFromLogic();

        // restore previous PLC running state (RUN/STOP) so device stays in same mode after deploy
        setPlcStatus(prevStatus, false);

        request->send(200, "text/plain", "Logic saved and deployed");
        broadcastLog("info", "Logic deployed successfully");
      } else {
        Serial.printf("[ERR %lu] /save: failed to open file for writing\n", millis());
        setPlcError("Failed to save logic file");
        // try to restore previous state anyway
        setPlcStatus(prevStatus, false);
        request->send(500, "text/plain", "Failed to save logic file");
      }
    } else {
      Serial.printf("[ERR %lu] /save: no logic parameter\n", millis());
      setPlcError("No logic data received");
      // restore previous state
      setPlcStatus(prevStatus, false);
      request->send(400, "text/plain", "No logic data received");
    }
  });

  // 상태 정보 요청
  server.on("/status", HTTP_GET, [](AsyncWebServerRequest *request){
    DynamicJsonDocument doc(512);
    doc["status"] = plcStatus == RUN ? "RUN" : plcStatus == STOP ? "STOP" :
                    plcStatus == ERROR ? "ERROR" : "RESET";
    doc["uptime"] = millis();
    doc["nodes"] = nodeCount;
    
    // WiFi 정보 추가
    JsonObject wifi = doc.createNestedObject("wifi");
    wifi["ap_ip"] = WiFi.softAPIP().toString();
    wifi["sta_connected"] = staConnected;
    if (staConnected) {
      wifi["sta_ip"] = staIP.toString();
      wifi["sta_ssid"] = WiFi.SSID();  // Connected SSID
      wifi["sta_rssi"] = WiFi.RSSI();  // Signal strength
    }
    // indicate whether STA credentials are saved in Preferences
    wifi["saved"] = prefs.getBool("sta_saved", false);

    String response;
    serializeJson(doc, response);
    request->send(200, "application/json", response);
  });

  // WiFi 스캔 요청
  server.on("/wifi/scan", HTTP_GET, [](AsyncWebServerRequest *request){
    Serial.println("[DBG] WiFi scan requested");
    
    // 현재 WiFi 모드 확인
    WiFiMode_t currentMode = WiFi.getMode();
    Serial.printf("[DBG] Current WiFi mode: %d (1=STA, 2=AP, 3=AP+STA)\n", currentMode);
    
    // AP+STA 모드가 아니면 변경
    if (currentMode != WIFI_AP_STA) {
      Serial.println("[DBG] Changing to AP+STA mode");
      WiFi.mode(WIFI_AP_STA);
      delay(200);
    }
    
    // 동기 스캔 (완료될 때까지 대기)
    Serial.println("[DBG] Starting WiFi scan...");
    int n = WiFi.scanNetworks(true);  // true = async mode를 시작
    
    // 스캔 완료 대기 (최대 15초)
    unsigned long scanStartTime = millis();
    const unsigned long SCAN_TIMEOUT = 15000;
    int scanResult = -1;
    
    while (millis() - scanStartTime < SCAN_TIMEOUT) {
      scanResult = WiFi.scanComplete();
      if (scanResult >= 0) {
        n = scanResult;
        break;
      }
      delay(100);
    }
    
    if (scanResult < 0) {
      Serial.println("[ERR] WiFi scan timeout or failed!");
    } else {
      Serial.printf("[DBG] Scan complete: found %d networks\n", n);
    }
    
    DynamicJsonDocument doc(2048);
    JsonArray networks = doc.createNestedArray("networks");
    
    if (n >= 0) {
      for (int i = 0; i < n; ++i) {
        JsonObject net = networks.createNestedObject();
        String ssid = WiFi.SSID(i);
        int32_t rssi = WiFi.RSSI(i);
        uint8_t channel = WiFi.channel(i);
        uint8_t secure = WiFi.encryptionType(i);
        
        net["ssid"] = ssid.isEmpty() ? "(hidden)" : ssid;
        net["rssi"] = rssi;
        net["channel"] = channel;
        net["secure"] = (secure != WIFI_AUTH_OPEN) ? 1 : 0;
        
        Serial.printf("[DBG]   Network: %s, RSSI: %d, Secure: %d\n", 
                      ssid.isEmpty() ? "(hidden)" : ssid.c_str(), rssi, secure != WIFI_AUTH_OPEN);
      }
    } else {
      Serial.println("[ERR] WiFi scan result is invalid");
      doc["error"] = "Scan failed - please try again";
    }
    
    // 스캔 결과 정리
    WiFi.scanDelete();
    
    String response;
    serializeJson(doc, response);
    Serial.printf("[DBG] Returning %d networks\n", networks.size());
    request->send(200, "application/json", response);
  });

  // WiFi 연결 요청
  server.on("/wifi/connect", HTTP_POST, [](AsyncWebServerRequest *request){
    if (request->hasParam("ssid", true) && request->hasParam("password", true)) {
      String ssid = request->getParam("ssid", true)->value();
      String password = request->getParam("password", true)->value();
      
      Serial.printf("[DBG] WiFi connect requested: SSID=%s\n", ssid.c_str());
      
      // AP+STA 모드 확인
      if (WiFi.getMode() != WIFI_AP_STA) {
        Serial.println("[DBG] Ensuring AP+STA mode for connection");
        WiFi.mode(WIFI_AP_STA);
        delay(200);
      }
      
      // STA 연결 시도
      WiFi.begin(ssid.c_str(), password.c_str());
      
      // 15초(30회 * 500ms) 대기
      int attempts = 0;
      const int MAX_ATTEMPTS = 30;
      while (WiFi.status() != WL_CONNECTED && attempts < MAX_ATTEMPTS) {
        delay(500);
        attempts++;
        Serial.printf("[DBG] Connection attempt %d/%d, status=%d\n", attempts, MAX_ATTEMPTS, WiFi.status());
      }
      
      if (WiFi.status() == WL_CONNECTED) {
        staConnected = true;
        staIP = WiFi.localIP();
        int32_t rssi = WiFi.RSSI();
        Serial.printf("[DBG] WiFi connected! IP: %s, RSSI: %d\n", staIP.toString().c_str(), rssi);
        
        DynamicJsonDocument doc(256);
        doc["success"] = true;
        doc["ip"] = staIP.toString();
        doc["rssi"] = rssi;
        doc["ssid"] = ssid;
        
        String response;
        serializeJson(doc, response);
        request->send(200, "application/json", response);
        broadcastLog("system", String("WiFi Connected: ") + staIP.toString());
        // Persist STA credentials only if client asked to save (save=1)
        if (request->hasParam("save", true)) {
          String saveVal = request->getParam("save", true)->value();
          if (saveVal == "1") {
            prefs.putString("sta_ssid", ssid);
            prefs.putString("sta_pass", password);
            prefs.putBool("sta_saved", true);
            Serial.println("[DBG] STA credentials saved per request");
          }
        }
      } else {
        staConnected = false;
        Serial.printf("[ERR] WiFi connection failed (status=%d)\n", WiFi.status());
        
        DynamicJsonDocument doc(256);
        doc["success"] = false;
        doc["error"] = "Connection timeout";
        doc["status"] = (int)WiFi.status();
        
        String response;
        serializeJson(doc, response);
        request->send(200, "application/json", response);
        broadcastLog("error", "WiFi Connection Failed");
      }
    } else {
      request->send(400, "application/json", "{\"success\":false,\"error\":\"Missing SSID or password\"}");
    }
  });

  // WiFi settings GET/POST (persist UI settings into Preferences)
  server.on("/wifi/settings", HTTP_GET, [](AsyncWebServerRequest *request){
    DynamicJsonDocument doc(256);
    // interval stored as string (ms) for simplicity
    String intervalStr = prefs.getString("wifi_retry_ms", "5000");
    int attempts = prefs.getInt("wifi_attempts", 3);
    bool saveDefault = prefs.getBool("wifi_save_default", true);
    doc["interval_ms"] = (unsigned long) intervalStr.toInt();
    doc["attempts"] = attempts;
    doc["save_default"] = saveDefault;
    String resp; serializeJson(doc, resp);
    request->send(200, "application/json", resp);
  });

  server.on("/wifi/settings", HTTP_POST, [](AsyncWebServerRequest *request){
    DynamicJsonDocument doc(256);
    // Accept form-encoded params: interval_ms, attempts, save_default
    if (request->hasParam("interval_ms", true)) {
      String v = request->getParam("interval_ms", true)->value();
      prefs.putString("wifi_retry_ms", v);
    }
    if (request->hasParam("attempts", true)) {
      int a = request->getParam("attempts", true)->value().toInt();
      prefs.putInt("wifi_attempts", a);
    }
    if (request->hasParam("save_default", true)) {
      String s = request->getParam("save_default", true)->value();
      prefs.putBool("wifi_save_default", s == "1");
    }
    doc["success"] = true;
    String resp; serializeJson(doc, resp);
    request->send(200, "application/json", resp);
  });

  // WiFi 연결 해제
  server.on("/wifi/disconnect", HTTP_GET, [](AsyncWebServerRequest *request){
    WiFi.disconnect(true);  // true = turn off WiFi radio
    staConnected = false;
    Serial.println("[DBG] WiFi disconnected");
    
    DynamicJsonDocument doc(256);
    doc["success"] = true;
    doc["message"] = "WiFi disconnected";
    
    String response;
    serializeJson(doc, response);
    request->send(200, "application/json", response);
    broadcastLog("system", "WiFi disconnected");
    // Optional: clear saved STA credentials when requested (e.g., /wifi/disconnect?forget=1)
    if (request->hasParam("forget")) {
      String v = request->getParam("forget")->value();
      if (v == "1") {
        prefs.remove("sta_ssid");
        prefs.remove("sta_pass");
        prefs.remove("sta_saved");
        Serial.println("[DBG] Cleared saved STA credentials (forget=1)");
        broadcastLog("system", "Saved STA credentials cleared");
      }
    }
  });
}

// Note: automatic STA connect/check logic removed. STA connections are handled
// exclusively through the HTTP endpoints (/wifi/connect, /wifi/disconnect)

// ============ LOGIC FILE FUNCTIONS ============
// 로직 파일 로드
void loadLogicFile() {
  Serial.printf("[DBG %lu] loadLogicFile: start\n", millis());
  if (LittleFS.exists("/logic.json")) {
    Serial.println("[DBG] /logic.json exists, trying to open");
    File f = LittleFS.open("/logic.json", "r");
    if (f) {
      String js = f.readString(); f.close();
      Serial.printf("[DBG %lu] loadLogicFile: file read, len=%d\n", millis(), (int)js.length());
      handleSave(js);
      Serial.println("[DBG] loadLogicFile: loaded successfully");
      return;
    } else {
      Serial.println("[DBG] loadLogicFile: Failed to open file (open returned null)");
    }
  } else {
    Serial.println("[DBG] loadLogicFile: /logic.json does not exist");
  }
  Serial.println("Logic Load Fail: 초기화");
  // Clear logic
  nodeCount = 0;
  linkCount = 0;
  memset(nodeOutputs, 0, sizeof(nodeOutputs));
  memset(nodePrevOutputs, 0, sizeof(nodePrevOutputs));
  setPlcStatus(STOP, false);
}

// ============ SETUP ============
void setup() {
  Serial.begin(115200);
  delay(1000);

  Serial.println("\n\nESP32 S3 WEB PLC v1.0");
  Serial.println("AP Mode + WebSocket + Ladder Logic");
  Serial.println("(ESP32_PLC_Simplified.ino logic integrated)");

  // Preferences 초기화
  prefs.begin("plc", false);

  // FastLED 초기화
  FastLED.addLeds<WS2812B, PLC_LED_PIN, GRB>(plc_leds, PLC_NUM_LEDS);
  FastLED.setBrightness(50);
  fill_solid(plc_leds, PLC_NUM_LEDS, CRGB::Blue); FastLED.show();

  // LittleFS 초기화
  if (!LittleFS.begin(true)) {
    Serial.println("[ERR] LittleFS mount failed");
    setPlcError("LittleFS mount failed");
    return;
  }
  Serial.println("[DBG] LittleFS mounted");

  // WiFi AP + STA 모드 설정
  Serial.println("[DBG] Setting WiFi mode to AP+STA");
  WiFi.mode(WIFI_AP_STA);
  delay(100);  // AP 모드 안정화 대기
  
  // AP 모드 시작
  if (WiFi.softAP(WIFI_SSID, WIFI_PASSWORD)) {
    Serial.printf("[DBG] AP started: %s\n", WIFI_SSID);
    Serial.print("[DBG] AP IP: ");
    Serial.println(WiFi.softAPIP());
  } else {
    Serial.println("[ERR] AP start failed");
    setPlcError("WiFi AP start failed");
    return;
  }
  
  delay(200);  // AP 시작 안정화 대기

  // --- 자동 STA 재연결: Preferences에 저장된 SSID/PW가 있으면 AP+STA 모드에서 자동으로 연결 시도
  String savedSsid = prefs.getString("sta_ssid", "");
  String savedPass = prefs.getString("sta_pass", "");
  if (savedSsid.length() > 0) {
    Serial.printf("[DBG] Found saved STA credentials: SSID=%s, attempting auto-connect...\n", savedSsid.c_str());
    // STA 연결 시도 (논블로킹 방식)
    WiFi.begin(savedSsid.c_str(), savedPass.c_str());
    
    // 최대 10초 대기 (블로킹 시간 단축)
    unsigned long start = millis();
    const unsigned long AUTO_CONNECT_TIMEOUT = 10000;
    int dotCount = 0;
    
    while (WiFi.status() != WL_CONNECTED && (millis() - start) < AUTO_CONNECT_TIMEOUT) {
      delay(200);  // 더 짧은 대기 시간
      Serial.print('.');
      dotCount++;
      if (dotCount % 10 == 0) {
        Serial.printf(" [%lu ms]\n", millis() - start);
      }
    }
    Serial.println();
    
    if (WiFi.status() == WL_CONNECTED) {
      staConnected = true;
      staIP = WiFi.localIP();
      Serial.printf("[DBG] Auto STA connected IP: %s, RSSI: %d dBm\n", staIP.toString().c_str(), WiFi.RSSI());
      broadcastLog("system", String("Auto WiFi Connected: ") + staIP.toString());
    } else {
      staConnected = false;
      Serial.println("[DBG] Auto STA connect failed or timed out");
      Serial.printf("[DBG] WiFi status: %d\n", WiFi.status());
      broadcastLog("system", "Auto WiFi connect failed or timed out");
      // 연결 실패 시 STA 모드 해제 (AP 안정성 향상)
      WiFi.disconnect(false);  // false = AP는 유지
      delay(100);
    }
  }


  // PLC 메모리 초기화
  initMemory();

  // Preferences에서 PLC 상태 로드
  PLCStatus savedStatus = loadPlcStatusFromPrefs();
  Serial.printf("[DBG] Loaded saved status: %d\n", (int)savedStatus);
  setPlcStatus(savedStatus, false); // don't persist again

  // 로직 파일 로드
  loadLogicFile();

  // WebSocket 설정
  ws.onEvent(onWsEvent);
  server.addHandler(&ws);

  // HTTP 엔드포인트 설정
  setupHttpEndpoints();

  // 서버 시작
  server.begin();
  Serial.println("[DBG] Web server started");

  // 초기 LED 상태
  fill_solid(plc_leds, PLC_NUM_LEDS, CRGB::Blue); FastLED.show();
  Serial.printf("[DBG %lu] setup() done\n", millis());
}

// ============ LOOP ============
void loop() {
  ws.cleanupClients();

  // Note: automatic STA periodic checking removed; STA status is updated
  // by the /wifi/connect and /wifi/disconnect handlers.

  // PLC 스캔 실행 (10ms 주기)
  if (plcStatus == RUN) {
    unsigned long now = millis();
    if (now - lastScanTime >= SCAN_INTERVAL) {
      lastScanTime = now;
      executeScan();
      sendPlcStatus(false); // rate-limited
    }
  }

  // LED 업데이트 (필요시)
  if (ledUpdateNeeded) {
    updatePlcLed();
    ledUpdateNeeded = false;
  }
}