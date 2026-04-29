/**
 * server.js â€“ libgpiod-backed backend with automation + Wiegand + OSDP + NFC + bridge + emulations persistence + FORMAT API + GPIO QUEUE
 * Run with: sudo node server.js
 *
 * Notes:
 * - PN532 (NFC) initialization is OPT-IN. Set ENABLE_PN532=1 to enable.
 * - GPIO Queue Manager prevents IOplus board lockups from command flooding
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { spawn, execFile, exec } = require('child_process');
const { promisify } = require('util');
const EventEmitter = require('events');
const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');

const execAsync = promisify(exec);

// ============================================
// IOplusController Class (Integrated)
// ============================================
class IOplusController {
  constructor(maxBoards = 1) {
    this.maxBoards = maxBoards;
    this.relaysPerBoard = 8;
    this.totalRelays = maxBoards * this.relaysPerBoard;
  }

  async executeCommand(board, command, retries = 3) {
    if (this.maxBoards === 1) {
      board = 0;
    }
    
    const cmd = `timeout 5 ioplus ${board} ${command}`;
    let lastError;
    
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const { stdout, stderr } = await execAsync(cmd);
        if (stderr && stderr.trim()) {
          throw new Error(stderr.trim());
        }
        
        if (attempt > 0) {
          console.log(`[IOplus] Command succeeded on attempt ${attempt + 1}`);
        }
        
        return stdout.trim();
      } catch (error) {
        lastError = error;
        
        const stack = board;
        if (error.message.includes('No IOplus card detected')) {
          throw new Error(`Board ${stack} not detected`);
        }
        
        if (attempt < retries - 1) {
          const backoffMs = Math.min(100 * Math.pow(2, attempt), 500);
          console.warn(`[IOplus] I2C error on attempt ${attempt + 1}, retrying in ${backoffMs}ms...`);
          await new Promise(resolve => setTimeout(resolve, backoffMs));
        }
      }
    }
    
    console.error(`[IOplus] Command failed after ${retries} attempts:`, lastError.message);
    throw new Error(`Command failed after ${retries} attempts: ${lastError.message}`);
  }

  pinToRelay(pin) {
    if (pin < 0 || pin >= this.totalRelays) {
      throw new Error(`Pin ${pin} out of range (0-${this.totalRelays - 1}). You only have 1 board with 8 relays.`);
    }
    
    const board = 0;
    const relay = pin + 1;
    
    return { board, relay };
  }

  relayToPin(board, relay) {
    if (board !== 0) {
      throw new Error(`Board ${board} doesn't exist. Only board 0 is available.`);
    }
    if (relay < 1 || relay > this.relaysPerBoard) {
      throw new Error(`Relay ${relay} out of range (1-${this.relaysPerBoard})`);
    }
    return relay - 1;
  }

  async setRelay(pin, state) {
    const { board, relay } = this.pinToRelay(pin);
    
    try {
      await this.executeCommand(board, `relwr ${relay} ${state ? 1 : 0}`);
      
      console.log(`[IOplus] Relay ${pin} (Board ${board}, Relay ${relay}) -> ${state ? 'ON' : 'OFF'}`);
      
      return {
        success: true,
        pin,
        board,
        relay,
        state
      };
    } catch (error) {
      console.error(`[IOplus] Failed to set relay:`, error.message);
      throw error;
    }
  }

  async getRelay(pin) {
    const { board, relay } = this.pinToRelay(pin);
    
    try {
      const result = await this.executeCommand(board, `relrd ${relay}`);
      const state = parseInt(result) === 1;
      
      return {
        success: true,
        pin,
        board,
        relay,
        state
      };
    } catch (error) {
      console.error(`[IOplus] Failed to read relay:`, error.message);
      throw error;
    }
  }

  async readOptoInput(pin) {
    const board = 0;
    const input = pin + 1;
    
    if (pin < 0 || pin >= 8) {
      throw new Error(`Input pin ${pin} out of range (0-7)`);
    }
    
    try {
      const result = await this.executeCommand(board, `optrd ${input}`);
      const state = parseInt(result) === 1;
      
      return {
        success: true,
        pin,
        board,
        input,
        state,
        type: 'opto'
      };
    } catch (error) {
      console.error(`[IOplus] Failed to read opto input:`, error.message);
      throw error;
    }
  }

  /**
   * Read analog input (ADC) - returns voltage in volts
   * Uses IOplus adcrd command for 0-10V analog inputs
   */
  async readAnalogInput(pin, threshold = 2500) {
    const board = 0;
    const channel = pin + 1; // ADC channels are 1-indexed (1-8)
    
    if (pin < 0 || pin >= 8) {
      throw new Error(`Analog input pin ${pin} out of range (0-7)`);
    }
    
    try {
      // Read raw ADC value (returns voltage in volts)
      const result = await this.executeCommand(board, `adcrd ${channel}`);
      const volts = parseFloat(result);
      
      if (isNaN(volts)) {
        throw new Error(`Invalid ADC reading: ${result}`);
      }
      
      // Convert to millivolts for easier comparison
      const millivolts = Math.round(volts * 1000);
      
      // Digital state based on threshold (default 2.5V = 2500mV)
      const state = millivolts >= threshold;
      
      return {
        success: true,
        pin,
        board,
        channel,
        volts: parseFloat(volts.toFixed(3)),
        millivolts,
        threshold,
        state,
        type: 'analog'
      };
    } catch (error) {
      console.error(`[IOplus] Failed to read analog input:`, error.message);
      throw error;
    }
  }

  /**
   * Read all analog inputs
   */
  async readAllAnalogInputs(threshold = 2500) {
    const results = [];
    for (let i = 0; i < 8; i++) {
      try {
        // Add 50ms delay between reads to reduce I2C load
        if (i > 0) {
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        const result = await this.readAnalogInput(i, threshold);
        results.push(result);
      } catch (error) {
        results.push({ success: false, pin: i, error: error.message, type: 'analog' });
      }
    }
    
    return results;
  }

  async pulseRelay(pin, durationMs = 500) {
    await this.setRelay(pin, true);
    return new Promise((resolve) => {
      setTimeout(async () => {
        await this.setRelay(pin, false);
        resolve({ success: true, pin, duration: durationMs });
      }, durationMs);
    });
  }

  async setAllRelays(states) {
    if (!Array.isArray(states) || states.length !== this.totalRelays) {
      throw new Error(`Expected array of ${this.totalRelays} states`);
    }
    
    const results = [];
    for (let i = 0; i < states.length; i++) {
      try {
        if (i > 0) {
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        const result = await this.setRelay(i, states[i]);
        results.push(result);
      } catch (error) {
        results.push({ success: false, pin: i, error: error.message });
      }
    }
    
    return results;
  }

  async getAllRelays() {
    const results = [];
    for (let i = 0; i < this.totalRelays; i++) {
      try {
        if (i > 0) {
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        const result = await this.getRelay(i);
        results.push(result);
      } catch (error) {
        results.push({ success: false, pin: i, error: error.message });
      }
    }
    
    return results;
  }

  async readAllOptoInputs() {
    const results = [];
    for (let i = 0; i < 8; i++) {
      try {
        if (i > 0) {
          await new Promise(resolve => setTimeout(resolve, 50));
        }
        const result = await this.readOptoInput(i);
        results.push(result);
      } catch (error) {
        results.push({ success: false, pin: i, error: error.message });
      }
    }
    
    return results;
  }

  async healthCheck() {
    try {
      await this.executeCommand(0, 'relrd 1');
      return { 
        healthy: true, 
        message: 'I2C bus responding normally',
        board: 0
      };
    } catch (error) {
      return { 
        healthy: false, 
        message: error.message,
        board: 0
      };
    }
  }
}

// ============================================
// GPIOQueueManager Class (Integrated)
// ============================================
class GPIOQueueManager extends EventEmitter {
  constructor(ioplusController, options = {}) {
    super();
    
    this.controller = ioplusController;
    this.queue = [];
    this.processing = false;
    this.paused = false;
    
    this.config = {
      maxQueueSize: options.maxQueueSize || 100,
      minDelayBetweenCommands: options.minDelayBetweenCommands || 50,
      maxConcurrentRequests: options.maxConcurrentRequests || 1,
      commandTimeout: options.commandTimeout || 5000,
      healthCheckInterval: options.healthCheckInterval || 10000,
      maxConsecutiveFailures: options.maxConsecutiveFailures || 5,
      burstProtectionWindow: options.burstProtectionWindow || 1000,
      maxBurstCommands: options.maxBurstCommands || 50
    };
    
    this.stats = {
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      queuedRequests: 0,
      droppedRequests: 0,
      consecutiveFailures: 0,
      lastSuccessTime: Date.now(),
      commandsInWindow: [],
      boardHealthy: true
    };
    
    // ⚠️ DISABLED - Health monitoring does I2C reads every 30s which can
    // contribute to gradual bus degradation. Use /api/gpio/health for manual checks.
    // this.startHealthMonitoring();
    console.log('[Queue] Health monitoring DISABLED to reduce I2C traffic');
  }

  async enqueue(operation, ...args) {
    if (!this.checkBurstProtection()) {
      this.stats.droppedRequests++;
      throw new Error('Rate limit exceeded - too many commands in short time window');
    }
    
    if (this.queue.length >= this.config.maxQueueSize) {
      this.stats.droppedRequests++;
      throw new Error(`Queue full (${this.config.maxQueueSize} items)`);
    }
    
    return new Promise((resolve, reject) => {
      const request = {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        operation,
        args,
        resolve,
        reject,
        timestamp: Date.now(),
        attempts: 0
      };
      
      this.queue.push(request);
      this.stats.queuedRequests++;
      this.stats.totalRequests++;
      
      if (!this.processing && !this.paused) {
        this.processQueue();
      }
    });
  }

  checkBurstProtection() {
    const now = Date.now();
    const windowStart = now - this.config.burstProtectionWindow;
    
    this.stats.commandsInWindow = this.stats.commandsInWindow.filter(
      timestamp => timestamp > windowStart
    );
    
    if (this.stats.commandsInWindow.length >= this.config.maxBurstCommands) {
      console.warn(`[Queue] Burst protection triggered: ${this.stats.commandsInWindow.length} commands in ${this.config.burstProtectionWindow}ms window`);
      return false;
    }
    
    this.stats.commandsInWindow.push(now);
    return true;
  }

  async processQueue() {
    if (this.processing || this.paused || this.queue.length === 0) {
      return;
    }
    
    this.processing = true;
    
    while (this.queue.length > 0 && !this.paused) {
      const request = this.queue.shift();
      
      try {
        if (!this.stats.boardHealthy) {
          throw new Error('Board unhealthy - waiting for recovery');
        }
        
        const result = await this.executeWithTimeout(request);
        
        this.stats.successfulRequests++;
        this.stats.consecutiveFailures = 0;
        this.stats.lastSuccessTime = Date.now();
        request.resolve(result);
        
        if (this.queue.length > 0) {
          await this.delay(this.config.minDelayBetweenCommands);
        }
        
      } catch (error) {
        this.stats.failedRequests++;
        this.stats.consecutiveFailures++;
        
        console.error(`[Queue] Command failed (${this.stats.consecutiveFailures} consecutive):`, error.message);
        
        if (this.stats.consecutiveFailures >= this.config.maxConsecutiveFailures) {
          console.error('[Queue] Board appears to be locked up - attempting recovery');
          this.stats.boardHealthy = false;
          this.emit('board-lockup', { consecutiveFailures: this.stats.consecutiveFailures });
          await this.attemptBoardRecovery();
        }
        
        request.reject(error);
      }
    }
    
    this.processing = false;
  }

  async executeWithTimeout(request) {
    return Promise.race([
      this.controller[request.operation](...request.args),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Command timeout')), this.config.commandTimeout)
      )
    ]);
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async attemptBoardRecovery() {
    console.log('[Queue] Attempting board recovery...');
    this.paused = true;
    
    try {
      console.log('[Queue] Step 1: Waiting for operations to clear...');
      await this.delay(2000);
      
      console.log('[Queue] Step 2: Testing board communication...');
      try {
        const health = await this.controller.healthCheck();
        if (health.healthy) {
          console.log('[Queue] Board recovered! Resuming operations.');
          this.stats.boardHealthy = true;
          this.stats.consecutiveFailures = 0;
          this.paused = false;
          this.emit('board-recovered');
          return true;
        }
      } catch (err) {
        console.log('[Queue] Health check failed:', err.message);
      }
      
      console.log('[Queue] Step 3: Requesting I2C bus reset...');
      this.emit('request-i2c-reset');
      await this.delay(3000);
      
      console.log('[Queue] Step 4: Testing board after I2C reset...');
      try {
        const health = await this.controller.healthCheck();
        if (health.healthy) {
          console.log('[Queue] Board recovered after I2C reset! Resuming operations.');
          this.stats.boardHealthy = true;
          this.stats.consecutiveFailures = 0;
          this.paused = false;
          this.emit('board-recovered');
          return true;
        }
      } catch (err) {
        console.log('[Queue] Health check failed after I2C reset:', err.message);
      }
      
      console.error('[Queue] Board recovery failed - POWER CYCLE REQUIRED');
      this.emit('board-needs-power-cycle');
      
      return false;
      
    } catch (error) {
      console.error('[Queue] Recovery attempt failed:', error);
      return false;
    }
  }

  async manualRecovery() {
    console.log('[Queue] Manual recovery triggered - resetting state...');
    
    this.stats.boardHealthy = true;
    this.stats.consecutiveFailures = 0;
    this.paused = false;
    this.queue = [];
    
    try {
      const health = await this.controller.healthCheck();
      if (health.healthy) {
        console.log('[Queue] Manual recovery successful!');
        this.emit('board-recovered');
        this.processQueue();
        return true;
      }
    } catch (err) {
      console.error('[Queue] Manual recovery failed - board still not responding');
      this.stats.boardHealthy = false;
      return false;
    }
  }

  startHealthMonitoring() {
    this.healthCheckTimer = setInterval(async () => {
      if (!this.stats.boardHealthy) {
        return;
      }
      
      try {
        const health = await this.controller.healthCheck();
        if (!health.healthy) {
          console.warn('[Queue] Health check failed - board may be unhealthy');
          this.stats.consecutiveFailures++;
        }
      } catch (err) {
        console.warn('[Queue] Health check error:', err.message);
        this.stats.consecutiveFailures++;
      }
    }, this.config.healthCheckInterval);
  }

  getStats() {
    return {
      ...this.stats,
      queueLength: this.queue.length,
      processing: this.processing,
      paused: this.paused,
      config: this.config
    };
  }

  pause() {
    this.paused = true;
    console.log('[Queue] Processing paused');
  }

  resume() {
    this.paused = false;
    console.log('[Queue] Processing resumed');
    if (this.queue.length > 0) {
      this.processQueue();
    }
  }

  clear() {
    const count = this.queue.length;
    this.queue = [];
    console.log(`[Queue] Cleared ${count} pending requests`);
    return count;
  }

  destroy() {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }
    this.clear();
  }
}

// ============================================
// InputMonitor Class - Polls inputs and emits changes via WebSocket
// ============================================
class InputMonitor extends EventEmitter {
  constructor(ioplusController, io, options = {}) {
    super();
    
    this.controller = ioplusController;
    this.io = io;
    this.enabled = false;
    this.pollInterval = null;
    
    // Configuration
    this.config = {
      pollIntervalMs: options.pollIntervalMs || 500,       // Poll every 500ms (safer default)
      analogThreshold: options.analogThreshold || 2500,    // 2.5V threshold for digital state
      enableOpto: options.enableOpto !== false,            // Monitor opto inputs by default
      enableAnalog: options.enableAnalog === true,         // Monitor analog inputs (disabled by default)
      debounceMs: options.debounceMs || 100,               // Debounce rapid changes
      readDelayMs: options.readDelayMs || 25               // Delay between I2C reads (was hardcoded 10ms)
    };
    
    // State tracking
    this.lastOptoStates = new Map();    // Map<pin, boolean>
    this.lastAnalogStates = new Map();  // Map<pin, { state: boolean, millivolts: number }>
    this.lastChangeTime = new Map();    // Map<pin, timestamp> for debouncing
    
    console.log('[InputMonitor] Initialized with config:', JSON.stringify(this.config));
  }

  /**
   * Start monitoring inputs
   */
  start() {
    if (this.enabled) {
      console.log('[InputMonitor] Already running');
      return;
    }
    
    this.enabled = true;
    console.log('[InputMonitor] Starting input monitoring...');
    
    // Initial read to populate state
    this.pollInputs().catch(err => {
      console.error('[InputMonitor] Initial poll failed:', err.message);
    });
    
    // Start polling interval
    this.pollInterval = setInterval(() => {
      this.pollInputs().catch(err => {
        console.error('[InputMonitor] Poll error:', err.message);
      });
    }, this.config.pollIntervalMs);
    
    console.log('[InputMonitor] ✓ Monitoring started');
    this.emit('started');
  }

  /**
   * Stop monitoring inputs
   */
  stop() {
    if (!this.enabled) {
      return;
    }
    
    this.enabled = false;
    
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    
    console.log('[InputMonitor] Stopped');
    this.emit('stopped');
  }

  /**
   * Poll all inputs and emit changes
   */
  async pollInputs() {
    if (!this.enabled) return;
    
    const now = Date.now();
    
    // Poll opto inputs
    if (this.config.enableOpto) {
      try {
        for (let pin = 0; pin < 8; pin++) {
          try {
            const result = await this.controller.readOptoInput(pin);
            const lastState = this.lastOptoStates.get(pin);
            
            // Check for state change with debounce
            if (lastState !== result.state) {
              const lastChange = this.lastChangeTime.get(`opto-${pin}`) || 0;
              
              if (now - lastChange >= this.config.debounceMs) {
                this.lastOptoStates.set(pin, result.state);
                this.lastChangeTime.set(`opto-${pin}`, now);
                
                // Emit WebSocket event
                const event = {
                  type: 'opto',
                  pin,
                  state: result.state ? 1 : 0,
                  timestamp: now
                };
                
                this.io.emit('input_state_change', event);
                this.emit('opto_change', event);
                
                console.log(`[InputMonitor] Opto ${pin}: ${result.state ? 'HIGH' : 'LOW'}`);
              }
            }
            
            // Delay between reads to prevent I2C bus saturation
            await new Promise(r => setTimeout(r, this.config.readDelayMs));
          } catch (err) {
            // Individual pin read failed, continue with others
          }
        }
      } catch (err) {
        console.error('[InputMonitor] Opto poll error:', err.message);
      }
    }
    
    // Poll analog inputs
    if (this.config.enableAnalog) {
      try {
        for (let pin = 0; pin < 8; pin++) {
          try {
            const result = await this.controller.readAnalogInput(pin, this.config.analogThreshold);
            const lastAnalog = this.lastAnalogStates.get(pin);
            
            // Check for state change (digital state based on threshold)
            const stateChanged = !lastAnalog || lastAnalog.state !== result.state;
            
            // Also check for significant voltage change (>100mV)
            const voltageChanged = lastAnalog && 
              Math.abs((lastAnalog.millivolts || 0) - result.millivolts) > 100;
            
            if (stateChanged || voltageChanged) {
              const lastChange = this.lastChangeTime.get(`analog-${pin}`) || 0;
              
              if (now - lastChange >= this.config.debounceMs) {
                this.lastAnalogStates.set(pin, {
                  state: result.state,
                  millivolts: result.millivolts
                });
                this.lastChangeTime.set(`analog-${pin}`, now);
                
                // Emit WebSocket event
                const event = {
                  type: 'analog',
                  pin,
                  state: result.state ? 1 : 0,
                  voltage: result.millivolts,
                  threshold: this.config.analogThreshold,
                  timestamp: now
                };
                
                this.io.emit('input_state_change', event);
                this.emit('analog_change', event);
                
                console.log(`[InputMonitor] Analog ${pin}: ${result.millivolts}mV → ${result.state ? 'HIGH' : 'LOW'}`);
              }
            }
            
            // Delay between reads to prevent I2C bus saturation
            await new Promise(r => setTimeout(r, this.config.readDelayMs));
          } catch (err) {
            // Individual pin read failed, continue with others
          }
        }
      } catch (err) {
        console.error('[InputMonitor] Analog poll error:', err.message);
      }
    }
  }

  /**
   * Get current state of all inputs
   */
  getCurrentStates() {
    const opto = {};
    const analog = {};
    
    this.lastOptoStates.forEach((state, pin) => {
      opto[pin] = state;
    });
    
    this.lastAnalogStates.forEach((data, pin) => {
      analog[pin] = data;
    });
    
    return {
      opto,
      analog,
      config: this.config,
      enabled: this.enabled
    };
  }

  /**
   * Update configuration
   */
  setConfig(newConfig) {
    Object.assign(this.config, newConfig);
    console.log('[InputMonitor] Config updated:', this.config);
    
    // Restart if running to apply new poll interval
    if (this.enabled && newConfig.pollIntervalMs !== undefined) {
      this.stop();
      this.start();
    }
  }

  /**
   * Force refresh of all input states
   */
  async refresh() {
    // Clear cached states to force re-read
    this.lastOptoStates.clear();
    this.lastAnalogStates.clear();
    this.lastChangeTime.clear();
    
    await this.pollInputs();
    
    return this.getCurrentStates();
  }
}

// ============================================
// Express & Socket.IO Setup
// ============================================

// optional pretty logger
let ServerLogger;
try {
  ServerLogger = require('./serverLogger');
} catch (e) {
  ServerLogger = null;
  console.warn('[Startup] ./serverLogger not found, falling back to console output.');
}

const AutomationManager = require('./automation/AutomationManager');
const WiegandManager = require('./wiegand/WiegandManager');
const OSDPManager = require('./osdp/OSDPManager');

// Log routes
let logRoutes = null;
let logSystemEvent = () => {};
let logAccessEvent = () => {};
let logSecurityEvent = () => {};
try {
  const _logs = require('./routes/logs');
  logRoutes = _logs.router || null;
  logSystemEvent = _logs.logSystemEvent || logSystemEvent;
  logAccessEvent = _logs.logAccessEvent || logAccessEvent;
  logSecurityEvent = _logs.logSecurityEvent || logSecurityEvent;
} catch (e) {
  console.warn('[Startup] ./routes/logs not found - logging routes/events disabled.');
}

// NFC
let PN532Manager = null;
try {
  PN532Manager = require('./nfc/PN532Manager');
} catch (e) {
  console.warn('[Startup] PN532Manager module not found - NFC will be unavailable if requested.');
}

// Format API
const formatRoutes = require('./routes/formats');

// Supervision routes (don't use gpioRoutes anymore - queue replaces it)
const supervisionRoutes = require('./routes/supervision-routes');

// ---- Global config ----
const CHIP = 'gpiochip0';

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET","POST","PUT","PATCH","DELETE"] } });

app.use(cors());
app.use(express.json());
app.use('/api/formats', formatRoutes);

app.use((req, res, next) => {
  req.io = io;
  next();
});

if (logRoutes) {
  app.use('/api/logs', logRoutes);
}

app.use('/api/supervision', supervisionRoutes);

// ============================================
// Initialize IOplus + Queue Manager
// ============================================
const ioplus = new IOplusController(1);
const gpioQueue = new GPIOQueueManager(ioplus, {
  maxQueueSize: 100,
  minDelayBetweenCommands: 100,   // 100ms between commands (was 50ms - doubled for safety)
  maxBurstCommands: 20,           // Reduced from 50 to prevent burst saturation
  burstProtectionWindow: 1000,
  maxConsecutiveFailures: 3,      // Fail faster to detect issues (was 5)
  healthCheckInterval: 30000      // Health check every 30s (was 10s)
});

// Event listeners
gpioQueue.on('board-lockup', async (data) => {
  console.error(`[Server] BOARD LOCKUP DETECTED - ${data.consecutiveFailures} consecutive failures`);
  console.error('[Server] Attempting automatic recovery...');
});

gpioQueue.on('request-i2c-reset', async () => {
  console.log('[Server] Executing I2C bus reset...');
  try {
    const { stdout, stderr } = await execAsync('bash ./reset-i2c-bus.sh');
    console.log('[Server] I2C reset output:', stdout);
    if (stderr) console.error('[Server] I2C reset errors:', stderr);
  } catch (error) {
    console.error('[Server] I2C reset failed:', error.message);
  }
});

gpioQueue.on('board-recovered', () => {
  console.log('[Server] âœ… Board recovered successfully!');
});

gpioQueue.on('board-needs-power-cycle', () => {
  console.error('[Server] âš ï¸  CRITICAL: Board needs POWER CYCLE - automatic recovery failed');
  console.error('[Server] Please manually power cycle the Raspberry Pi or trigger hardware reset');
});

console.log('[Server] GPIO Queue Manager initialized');
console.log('[Server] Burst protection: Max', gpioQueue.config.maxBurstCommands, 'commands per', gpioQueue.config.burstProtectionWindow + 'ms');
console.log('[Server] Queue size limit:', gpioQueue.config.maxQueueSize);
console.log('[Server] Min delay between commands:', gpioQueue.config.minDelayBetweenCommands + 'ms');

// ============================================
// Initialize Input Monitor for real-time input state tracking
// ============================================
const inputMonitor = new InputMonitor(ioplus, io, {
  pollIntervalMs: 1000,      // Poll every 1000ms (1 second - very conservative)
  analogThreshold: 2500,    // 2.5V threshold for digital state (millivolts)
  enableOpto: true,         // Monitor opto-isolated inputs
  enableAnalog: false,      // DISABLED - causes I2C bus saturation
  debounceMs: 100,          // 100ms debounce
  readDelayMs: 50           // 50ms between individual reads (very safe)
});

// InputMonitor will auto-start 10 seconds after server is ready
// This delay prevents I2C bus contention during OSDP/Wiegand/NFC initialization
// See delayed start at end of server.listen() callback
// inputMonitor.start();  // ← Moved to delayed auto-start

// Log input changes for debugging
inputMonitor.on('opto_change', (event) => {
  console.log(`[InputMonitor] Opto change: Pin ${event.pin} -> ${event.state ? 'HIGH' : 'LOW'}`);
});

inputMonitor.on('analog_change', (event) => {
  console.log(`[InputMonitor] Analog change: Pin ${event.pin} -> ${event.voltage}mV (${event.state ? 'HIGH' : 'LOW'})`);
});

console.log('[Server] Input Monitor initialized (will auto-start after 10s delay)');
console.log('[Server] Or use POST /api/gpio/monitor/start to start immediately');

// ---- Native Wiegand transmitter binary (optional) ----
const WIEGAND_TX_PATH = path.join(__dirname, 'bin', 'wiegand_tx');
const ALT_WIEGAND_TX_PATH = path.join(__dirname, 'wiegand', 'wiegand_tx');
const RESOLVED_WIEGAND_TX_PATH = fs.existsSync(WIEGAND_TX_PATH) ? WIEGAND_TX_PATH
  : (fs.existsSync(ALT_WIEGAND_TX_PATH) ? ALT_WIEGAND_TX_PATH : WIEGAND_TX_PATH);

const wiegandHistory = [];
const MAX_HISTORY = 100;

// ---- Sequences storage dir ----
const SEQUENCES_DIR = path.join(__dirname, 'data', 'sequences');
(async () => {
  try { await fsp.mkdir(SEQUENCES_DIR, { recursive: true }); }
  catch (e) { console.error('[Sequences] mkdir failed:', e.message); }
})();

function safeId(str='') { return String(str).replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 64) || String(Date.now()); }
async function loadJson(file) { return JSON.parse(await fsp.readFile(file, 'utf8')); }

class GPIOEventEmitter extends EventEmitter {}
const gpioEvents = new GPIOEventEmitter();

// Initialize systems
let automationManager = null;
let wiegandManager = null;
let osdpManager = null;
let nfcManager = null;
let nfcBridge = null;

const logger = ServerLogger ? new ServerLogger({
  style: process.env.NODE_ENV === 'production' ? 'compact' : 'modern',
  useColors: process.stdout.isTTY,
  useBox: true
}) : null;

function createServiceInfo(ok, details = null, summary = null) {
  return {
    ok: !!ok,
    details: details || null,
    summary: summary || (ok ? 'available' : 'unavailable')
  };
}

function logServerStatus() {
  const quick = {
    title: 'Server Status',
    port: process.env.PORT || 3001,
    services: {
      'Automation': { enabled: !!automationManager },
      'Wiegand': { enabled: !!wiegandManager },
      'OSDP': { enabled: !!osdpManager },
      'NFC': { enabled: !!(nfcManager && nfcManager.enabled) }
    },
    timestamp: new Date().toISOString()
  };
  if (logger && typeof logger.log === 'function') {
    logger.log(quick);
  } else {
    console.log('[Server Status]', quick);
  }
}

async function initializeWiegand() {
  if (wiegandManager) return wiegandManager;
  try {
    wiegandManager = new WiegandManager();
    await wiegandManager.initialize();
    console.log('[Wiegand] Helper initializeWiegand succeeded');
    return wiegandManager;
  } catch (err) {
    console.error('[Wiegand] Helper initializeWiegand failed:', err && err.message ? err.message : err);
    wiegandManager = null;
    throw err;
  }
}

async function initializeSystems() {
  try {
    wiegandManager = new WiegandManager();
    await wiegandManager.initialize();
    console.log('[Wiegand] âœ“ System initialized successfully');
  } catch (err) {
    console.error('[Wiegand] âœ— Failed to initialize:', err && err.message ? err.message : err);
    console.error('[Wiegand] Wiegand features will be disabled');
    wiegandManager = null;
  }

  try {
    osdpManager = new OSDPManager();
    await osdpManager.initialize();
    console.log('[OSDP] âœ“ System initialized successfully');

    if (osdpManager && typeof osdpManager.on === 'function') {
      osdpManager.on('card_read', (data) => {
        console.log('[OSDP] Card read event:', data);
        io.emit('osdp_card_read', data);

        try {
          logAccessEvent && logAccessEvent(`OSDP card read - reader ${data.readerId || 'unknown'} uid ${data.uid || data.card || 'n/a'}`, {
            readerId: data.readerId,
            uid: data.uid || null,
            facility: data.facility ?? null,
            card: data.card ?? null,
            method: 'osdp'
          });
        } catch (e) {}
      });
    }
  } catch (err) {
    console.error('[OSDP] âœ— Failed to initialize:', err && err.message ? err.message : err);
    console.error('[OSDP] OSDP features will be disabled');
    osdpManager = null;
  }

  try {
    const enableNfc = String(process.env.ENABLE_PN532 || '').trim() === '1';

    if (!enableNfc) {
      console.log('[NFC] PN532 initialization skipped (ENABLE_PN532 not set). To enable, set ENABLE_PN532=1');
      nfcManager = null;
    } else if (!PN532Manager) {
      console.warn('[NFC] PN532Manager module is missing; cannot initialize NFC.');
      nfcManager = null;
    } else {
      const i2cBus = Number(process.env.PN532_I2C_BUS || 1);
      const addrs = process.env.PN532_I2C_ADDRS
        ? process.env.PN532_I2C_ADDRS.split(',').map(s => Number(s.trim()))
        : [0x24, 0x48];

      nfcManager = new PN532Manager(io, { i2cBus, addresses: addrs });
      await nfcManager.initialize();

      if (nfcManager && nfcManager.enabled) {
        console.log('[NFC] âœ“ PN532 NFC Reader initialized successfully');

        nfcManager.on('card_read', (cardEvent) => {
          console.log(`[NFC] Card read: ${cardEvent.uid}`);
          io.emit('nfc:card', cardEvent);

          try {
            logAccessEvent && logAccessEvent(`NFC card read - uid ${cardEvent.uid}`, {
              uid: cardEvent.uid,
              type: cardEvent.type || null,
              method: 'nfc'
            });
          } catch (e) {}
        });

        nfcManager.on('error', (error) => {
          console.error('[NFC] Error:', error && error.message ? error.message : error);
          io.emit('nfc:error', { error: error && error.message ? error.message : String(error) });

          try {
            logSystemEvent && logSystemEvent('NFC error: ' + (error && error.message ? error.message : String(error)), 'error');
          } catch (e) {}
        });
      } else {
        console.warn('[NFC] PN532 disabled or not available after init');
        nfcManager = null;
      }
    }
  } catch (err) {
    console.error('[NFC] Failed to initialize:', err && err.message ? err.message : err);
    console.error('[NFC] NFC features will be disabled');
    nfcManager = null;
  }

  if (nfcManager && nfcManager.enabled && osdpManager) {
    try {
      nfcBridge = new (require('./nfc/NFCOSDPBridge'))(nfcManager, osdpManager);
      await nfcBridge.initialize();

      console.log('[NFC-OSDP Bridge] âœ“ Bridge initialized');
      console.log('[NFC-OSDP Bridge] NFC cards will be forwarded to OSDP readers');

      nfcBridge.on('card_sent', (data) => {
        console.log('[NFC-OSDP Bridge] Card forwarded to OSDP');
        io.emit('nfc:bridge:card_sent', data);

        try {
          logAccessEvent && logAccessEvent('NFC card forwarded to OSDP', { data });
        } catch (e) {}
      });

      nfcBridge.on('error', (data) => {
        console.error('[NFC-OSDP Bridge] Error:', data && data.error ? data.error : data);
        io.emit('nfc:bridge:error', data);

        try {
          logSystemEvent && logSystemEvent('NFC-OSDP Bridge error: ' + (data && data.error ? data.error : 'unknown'), 'error');
        } catch (e) {}
      });
    } catch (err) {
      console.error('[NFC-OSDP Bridge] Failed to initialize:', err && err.message ? err.message : err);
      nfcBridge = null;
    }
  } else {
    console.log('[NFC-OSDP Bridge] Skipped - NFC or OSDP not available');
  }

  try {
    automationManager = new AutomationManager(gpioEvents, wiegandManager);
    await automationManager.initialize();
    console.log('[Automation] âœ“ System initialized successfully');
  } catch (err) {
    console.error('[Automation] âœ— Failed to initialize:', err && err.message ? err.message : err);
    automationManager = null;
  }
}

initializeSystems().catch(err => {
  console.error('[Init] initializeSystems() threw:', err && err.stack ? err.stack : err);
});

app.use((req, _res, next) => { 
  try {
    const body = req.body && Object.keys(req.body).length ? req.body : undefined;
    console.log(`[REQ] ${req.method} ${req.url}`, body ? body : '');
  } catch (e) {
    console.log(`[REQ] ${req.method} ${req.url}`);
  }
  next(); 
});

const procs = new Map();
const states = new Map();

function killProc(pin) {
  const p = procs.get(pin);
  if (p && !p.killed) { 
    try { 
      process.kill(p.pid, 'SIGKILL'); 
    } catch (e) {}
  }
  procs.delete(pin);
}

function holdLevel(pin, value) {
  if (wiegandManager && typeof wiegandManager.isPinReserved === 'function' && wiegandManager.isPinReserved(pin)) {
    const error = `GPIO ${pin} is RESERVED for Wiegand transmission. Use /api/wiegand endpoints instead.`;
    console.error(`[GPIO] âœ— ${error}`);

    try {
      logSecurityEvent && logSecurityEvent(`Attempt to write reserved GPIO ${pin}`, 'warning', { pin });
    } catch (e) {}

    throw new Error(error);
  }

  killProc(pin);
  const args = ['-c', CHIP, `${pin}=${value}`];
  console.log(`[GPIO] Executing: gpioset ${args.join(' ')}`);
  
  const p = spawn('gpioset', args, { 
    stdio: ['ignore', 'pipe', 'pipe'] 
  });
  
  let stderrData = '';
  p.stderr.on('data', (data) => {
    stderrData += data.toString();
  });
  
  procs.set(pin, p);
  states.set(pin, value);
  
  gpioEvents.emit('gpio_change', { 
    pin, 
    value, 
    timestamp: Date.now() 
  });
  
  p.on('exit', (code) => { 
    if (procs.get(pin) === p) {
      procs.delete(pin);
      if (code !== 0) {
        console.error(`[GPIO] âœ— ERROR for pin ${pin}: Exit code ${code}`);
        if (stderrData.trim()) console.error(`[GPIO] stderr: ${stderrData.trim()}`);
      } else {
        console.log(`[GPIO] âœ“ Pin ${pin} successfully set to ${value}`);
      }
    }
  });
  
  p.on('error', (err) => {
    console.error(`[GPIO] Error spawning gpioset for pin ${pin}:`, err && err.message ? err.message : err);
  });
  
  console.log(`[GPIO] Pin ${pin} command sent (PID: ${p.pid})`);
}

function readLevel(pin) {
  return new Promise((resolve, reject) => {
    execFile('gpioget', ['-c', CHIP, String(pin)], (err, stdout, stderr) => {
      if (err) return reject(new Error(stderr || err.message));
      const txt = String(stdout).trim();
      if (txt === '0' || txt === '1') return resolve(parseInt(txt, 10));
      const m = txt.match(/=\s*(active|inactive)/i);
      if (m) return resolve(m[1].toLowerCase() === 'active' ? 1 : 0);
      resolve(states.get(pin) ?? 0);
    });
  });
}

async function pulse(pin, msec = 300) {
  if (wiegandManager && typeof wiegandManager.isPinReserved === 'function' && wiegandManager.isPinReserved(pin)) {
    try {
      logSecurityEvent && logSecurityEvent(`Attempt to pulse reserved GPIO ${pin}`, 'warning', { pin, msec });
    } catch (e) {}
    throw new Error(`GPIO ${pin} is RESERVED for Wiegand transmission`);
  }

  console.log(`[GPIO] Pulsing pin ${pin} for ${msec}ms`);
  holdLevel(pin, 1);
  await new Promise(r => setTimeout(r, Math.max(1, msec)));
  holdLevel(pin, 0);
}

gpioEvents.on('gpio_action', (data) => {
  const { pin, value } = data;
  console.log(`[Automation] Triggering GPIO ${pin} = ${value}`);
  try {
    holdLevel(pin, value);
  } catch (e) {
    console.error('[Automation] GPIO action blocked:', e && e.message ? e.message : e);
    try {
      logSecurityEvent && logSecurityEvent('Blocked automation GPIO action', 'warning', { pin, value, error: e && e.message ? e.message : String(e) });
    } catch (ee) {}
  }
});

// ============================================
// QUEUE-PROTECTED GPIO API ROUTES
// ============================================

app.post('/api/gpio/write', async (req, res) => {
  const { pin, value } = req.body;
  
  if (pin === undefined || value === undefined) {
    return res.status(400).json({ error: 'Missing pin or value' });
  }
  
  try {
    const result = await gpioQueue.enqueue('setRelay', pin, value === 1 || value === true);
    
    res.json({
      success: true,
      pin,
      value,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Write failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

app.get('/api/gpio/read/:pin', async (req, res) => {
  const pin = parseInt(req.params.pin);
  
  if (isNaN(pin)) {
    return res.status(400).json({ error: 'Invalid pin number' });
  }
  
  try {
    const result = await gpioQueue.enqueue('getRelay', pin);
    
    res.json({
      success: true,
      pin,
      state: result.state ? 1 : 0,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

app.get('/api/gpio/input/:pin', async (req, res) => {
  const pin = parseInt(req.params.pin);
  
  if (isNaN(pin)) {
    return res.status(400).json({ error: 'Invalid pin number' });
  }
  
  try {
    const result = await gpioQueue.enqueue('readOptoInput', pin);
    
    res.json({
      success: true,
      pin,
      state: result.state ? 1 : 0,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Input read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

// Analog input endpoint
app.get('/api/gpio/analog/:pin', async (req, res) => {
  const pin = parseInt(req.params.pin);
  const threshold = parseInt(req.query.threshold) || 2500;
  
  if (isNaN(pin)) {
    return res.status(400).json({ error: 'Invalid pin number' });
  }
  
  try {
    const result = await gpioQueue.enqueue('readAnalogInput', pin, threshold);
    
    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Analog input read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

// Read all opto inputs
app.get('/api/gpio/inputs/opto', async (req, res) => {
  try {
    const results = await gpioQueue.enqueue('readAllOptoInputs');
    
    res.json({
      success: true,
      type: 'opto',
      inputs: results
    });
  } catch (error) {
    console.error('[GPIO] Opto inputs read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

// Read all analog inputs
app.get('/api/gpio/inputs/analog', async (req, res) => {
  const threshold = parseInt(req.query.threshold) || 2500;
  
  try {
    const results = await gpioQueue.enqueue('readAllAnalogInputs', threshold);
    
    res.json({
      success: true,
      type: 'analog',
      threshold,
      inputs: results
    });
  } catch (error) {
    console.error('[GPIO] Analog inputs read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

// Read all inputs (both opto and analog)
app.get('/api/gpio/inputs/all', async (req, res) => {
  const threshold = parseInt(req.query.threshold) || 2500;
  
  try {
    const [optoResults, analogResults] = await Promise.all([
      gpioQueue.enqueue('readAllOptoInputs'),
      gpioQueue.enqueue('readAllAnalogInputs', threshold)
    ]);
    
    res.json({
      success: true,
      threshold,
      opto: optoResults,
      analog: analogResults,
      timestamp: Date.now()
    });
  } catch (error) {
    console.error('[GPIO] All inputs read failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

// Input monitor status and control
app.get('/api/gpio/monitor/status', (req, res) => {
  res.json({
    success: true,
    ...inputMonitor.getCurrentStates()
  });
});

app.post('/api/gpio/monitor/start', (req, res) => {
  inputMonitor.start();
  res.json({
    success: true,
    message: 'Input monitor started'
  });
});

app.post('/api/gpio/monitor/stop', (req, res) => {
  inputMonitor.stop();
  res.json({
    success: true,
    message: 'Input monitor stopped'
  });
});

app.post('/api/gpio/monitor/config', (req, res) => {
  const { pollIntervalMs, analogThreshold, enableOpto, enableAnalog, debounceMs, readDelayMs } = req.body;
  
  inputMonitor.setConfig({
    ...(pollIntervalMs !== undefined && { pollIntervalMs }),
    ...(analogThreshold !== undefined && { analogThreshold }),
    ...(enableOpto !== undefined && { enableOpto }),
    ...(enableAnalog !== undefined && { enableAnalog }),
    ...(debounceMs !== undefined && { debounceMs }),
    ...(readDelayMs !== undefined && { readDelayMs })
  });
  
  res.json({
    success: true,
    config: inputMonitor.config
  });
});

app.post('/api/gpio/monitor/refresh', async (req, res) => {
  try {
    const states = await inputMonitor.refresh();
    res.json({
      success: true,
      ...states
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.post('/api/gpio/pulse', async (req, res) => {
  const { pin, duration = 500 } = req.body;
  
  if (pin === undefined) {
    return res.status(400).json({ error: 'Missing pin' });
  }
  
  try {
    const result = await gpioQueue.enqueue('pulseRelay', pin, duration);
    
    res.json({
      success: true,
      pin,
      duration,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Pulse failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

app.post('/api/gpio/set', async (req, res) => {
  const { pin, value } = req.body;
  
  if (pin === undefined || value === undefined) {
    return res.status(400).json({ error: 'Missing pin or value' });
  }
  
  try {
    const result = await gpioQueue.enqueue('setRelay', pin, value === 1 || value === true);
    io.emit('gpio_state_change', { pin, value });
    res.json({
      success: true,
      pin,
      value,
      ...result
    });
  } catch (error) {
    console.error('[GPIO] Set failed:', error.message);
    res.status(500).json({
      error: error.message,
      queueStats: gpioQueue.getStats()
    });
  }
});

app.get('/api/gpio/states', (_req, res) => {
  const s = {}; 
  states.forEach((v, p) => s[p] = v);
  res.json({ success: true, states: s });
});

// Queue management endpoints
app.get('/api/gpio/queue/stats', (req, res) => {
  res.json(gpioQueue.getStats());
});

app.post('/api/gpio/queue/pause', (req, res) => {
  gpioQueue.pause();
  res.json({ success: true, message: 'Queue paused' });
});

app.post('/api/gpio/queue/resume', (req, res) => {
  gpioQueue.resume();
  res.json({ success: true, message: 'Queue resumed' });
});

app.post('/api/gpio/queue/clear', (req, res) => {
  const count = gpioQueue.clear();
  res.json({ success: true, message: `Cleared ${count} pending requests` });
});

app.post('/api/gpio/recovery/manual', async (req, res) => {
  console.log('[Server] Manual recovery requested');
  const success = await gpioQueue.manualRecovery();
  
  res.json({
    success,
    message: success ? 'Recovery successful' : 'Recovery failed - board still not responding',
    stats: gpioQueue.getStats()
  });
});

app.post('/api/gpio/reset-i2c', async (req, res) => {
  console.log('[Server] Manual I2C reset requested');
  
  try {
    const { stdout, stderr } = await execAsync('bash ./reset-i2c-bus.sh');
    
    res.json({
      success: true,
      message: 'I2C reset completed',
      output: stdout,
      errors: stderr || null
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'I2C reset failed',
      error: error.message
    });
  }
});

// ---- Health ----
app.get(['/api/health','/health'], (_req, res) => {
  res.json({ 
    success: true, 
    driver: 'libgpiod-tools', 
    chip: CHIP, 
    activePins: procs.size,
    automationEnabled: !!automationManager,
    wiegandEnabled: !!wiegandManager,
    osdpEnabled: !!osdpManager,
    nfcEnabled: !!(nfcManager && nfcManager.enabled),
    bridgeEnabled: !!(nfcBridge && typeof nfcBridge.isEnabled === 'function' && nfcBridge.isEnabled()),
    nativeTransmitter: fs.existsSync(RESOLVED_WIEGAND_TX_PATH),
    formatApiEnabled: true,
    queueEnabled: true,
    queueStats: gpioQueue.getStats(),
    timestamp: new Date().toISOString() 
  });
});

// ---- I2C Diagnostics - comprehensive status endpoint ----
app.get('/api/i2c/diagnostics', async (req, res) => {
  const uptime = process.uptime();
  const uptimeHours = (uptime / 3600).toFixed(2);
  const uptimeDays = (uptime / 86400).toFixed(3);
  
  let i2cDevices = [];
  try {
    const { stdout } = await execAsync('i2cdetect -y 1 2>/dev/null | grep -E "^[0-9]" | grep -oE "[0-9a-f]{2}" | head -20');
    i2cDevices = stdout.trim().split('\n').filter(x => x);
  } catch (e) {
    i2cDevices = ['scan failed'];
  }
  
  res.json({
    success: true,
    uptime: {
      seconds: Math.floor(uptime),
      hours: parseFloat(uptimeHours),
      days: parseFloat(uptimeDays),
      formatted: `${Math.floor(uptime/86400)}d ${Math.floor((uptime%86400)/3600)}h ${Math.floor((uptime%3600)/60)}m`
    },
    queue: gpioQueue.getStats(),
    inputMonitor: {
      enabled: inputMonitor.enabled,
      config: inputMonitor.config,
      pollCount: inputMonitor.lastOptoStates ? inputMonitor.lastOptoStates.size : 0
    },
    i2cBus: {
      devices: i2cDevices,
      ioPlusAddress: '0x28',
      expectedDevices: ['0x24', '0x28']
    },
    activeSubsystems: {
      wiegand: !!wiegandManager,
      osdp: !!osdpManager,
      nfc: !!(nfcManager && nfcManager.enabled),
      automation: !!automationManager
    },
    recommendations: gpioQueue.stats.boardHealthy 
      ? ['Board is healthy']
      : ['Board unhealthy - try POST /api/gpio/recovery/manual', 'Or POST /api/gpio/reset-i2c'],
    timestamp: new Date().toISOString()
  });
});

// ==============================================
// Sequences API
// ==============================================

app.get('/api/emulations', async (_req, res) => {
  try {
    const files = (await fsp.readdir(SEQUENCES_DIR)).filter(f => f.endsWith('.json'));
    const items = [];
    for (const f of files) {
      try {
        const j = await loadJson(path.join(SEQUENCES_DIR, f));
        items.push({
          id: j.id, name: j.name,
          createdAt: j.createdAt, updatedAt: j.updatedAt,
          stepsCount: Array.isArray(j.steps) ? j.steps.length : 0
        });
      } catch (_) {}
    }
    res.json({ success: true, items });
  } catch (e) {
    res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.get('/api/emulations/:id', async (req, res) => {
  try {
    const id = safeId(req.params.id);
    const file = path.join(SEQUENCES_DIR, `${id}.json`);
    const seq = await loadJson(file);
    res.json({ success: true, sequence: seq });
  } catch (e) {
    res.status(404).json({ success: false, error: 'Not found' });
  }
});

app.post('/api/emulations', async (req, res) => {
  try {
    const { id, name, steps } = req.body || {};
    if (!name || !Array.isArray(steps)) {
      return res.status(400).json({ success: false, error: 'name and steps required' });
    }
    const now = new Date().toISOString();
    const _id = safeId(id || name);
    const file = path.join(SEQUENCES_DIR, `${_id}.json`);
    let payload = { id: _id, name: String(name), steps, createdAt: now, updatedAt: now };
    try {
      const exists = await loadJson(file);
      payload.createdAt = exists.createdAt || now;
      payload.updatedAt = now;
    } catch {}
    await fsp.writeFile(file, JSON.stringify(payload, null, 2));
    res.json({ success: true, id: _id });
  } catch (e) {
    res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.delete('/api/emulations/:id', async (req, res) => {
  try {
    const id = safeId(req.params.id);
    const file = path.join(SEQUENCES_DIR, `${id}.json`);
    await fsp.unlink(file);
    res.json({ success: true });
  } catch (e) {
    res.status(404).json({ success: false, error: 'Not found' });
  }
});

app.get('/api/emulations/:id/export', async (req, res) => {
  try {
    const id = safeId(req.params.id);
    const file = path.join(SEQUENCES_DIR, `${id}.json`);
    const txt = await fsp.readFile(file, 'utf8');
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="${id}.json"`);
    res.status(200).send(txt);
  } catch (e) {
    res.status(404).json({ success: false, error: 'Not found' });
  }
});

app.post('/api/emulations/import', async (req, res) => {
  try {
    const payload = req.body;
    const list = Array.isArray(payload) ? payload : [payload];
    const saved = [];
    for (const item of list) {
      if (!item || !item.name || !Array.isArray(item.steps)) continue;
      const now = new Date().toISOString();
      const _id = safeId(item.id || item.name);
      const file = path.join(SEQUENCES_DIR, `${_id}.json`);
      const record = {
        id: _id,
        name: String(item.name),
        steps: item.steps,
        createdAt: item.createdAt || now,
        updatedAt: now
      };
      await fsp.writeFile(file, JSON.stringify(record, null, 2));
      saved.push(_id);
    }
    res.json({ success: true, imported: saved.length, ids: saved });
  } catch (e) {
    res.status(400).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

// ==================================================
// AUTOMATION API ROUTES
// ==================================================
app.get('/api/automation/rules', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  res.json({ success: true, rules: automationManager.getRules() });
});

app.get('/api/automation/stats', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  res.json({ success: true, stats: automationManager.getStats() });
});

app.post('/api/automation/rules', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    automationManager.addRule(req.body);
    res.json({ success: true });
  } catch (e) {
    res.status(400).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.put('/api/automation/rules/:id', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    automationManager.updateRule(req.params.id, req.body);
    res.json({ success: true });
  } catch (e) {
    res.status(400).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.delete('/api/automation/rules/:id', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    automationManager.removeRule(req.params.id);
    res.json({ success: true });
  } catch (e) {
    res.status(404).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.patch('/api/automation/rules/:id/enable', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    automationManager.enableRule(req.params.id);
    res.json({ success: true });
  } catch (e) {
    res.status(404).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.patch('/api/automation/rules/:id/disable', (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    automationManager.disableRule(req.params.id);
    res.json({ success: true });
  } catch (e) {
    res.status(404).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.post('/api/automation/reload', async (req, res) => {
  if (!automationManager) {
    return res.status(503).json({ success: false, error: 'Automation not initialized' });
  }
  try {
    await automationManager._loadRulesFromDisk();
    automationManager._applySchedules();
    const rules = automationManager.getRules();
    res.json({ success: true, reloadedRules: rules.length });
  } catch (e) {
    res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

// ==================================================
// WIEGAND API ROUTES
// ==================================================

const WIEGAND_INLINE_CONFIG = {
  ok: true,
  chip: CHIP,
  doors: [
    {
      door: 1,
      name: 'Door 1 (Reader A, shared w/ Door 3)',
      d0: 23,
      d1: 24,
      readerId: 'reader1'
    },
    {
      door: 2,
      name: 'Door 2 (Reader B, shared w/ Door 4)',
      d0: 22,
      d1: 23,
      readerId: 'reader2'
    }
  ],
  reserved: [2, 3, 4, 7, 8, 9, 10, 11, 14, 15]
};

function getDoorConfig(doorNumber) {
  const n = Number(doorNumber);
  return WIEGAND_INLINE_CONFIG.doors.find(d => d.door === n) || null;
}

function resolveWiegandTarget({ door, readerId }) {
  if (door != null) {
    const dc = getDoorConfig(door);
    if (!dc) throw new Error('Invalid door (must be 1 or 2)');
    return { readerId: dc.readerId, doorCfg: dc };
  }
  if (!readerId) throw new Error('readerId or door required');
  const doorCfg = WIEGAND_INLINE_CONFIG.doors.find(d => d.readerId === readerId) || null;
  return { readerId, doorCfg };
}

app.get('/api/wiegand/config', (_req, res) => {
  res.json(WIEGAND_INLINE_CONFIG);
});

app.get('/api/wiegand/status', (req, res) => {
  const nativeExists = fs.existsSync(RESOLVED_WIEGAND_TX_PATH);
  const nativeExecutable = nativeExists ? (() => {
    try {
      fs.accessSync(RESOLVED_WIEGAND_TX_PATH, fs.constants.X_OK);
      return true;
    } catch { return false; }
  })() : false;

  if (!wiegandManager) {
    return res.status(503).json({
      success: false,
      error: 'Wiegand system not initialized',
      nativeTransmitter: nativeExists,
      nativeExecutable
    });
  }
  res.json({ success: true, status: wiegandManager.getStatus(), nativeTransmitter: nativeExists, nativeExecutable });
});

app.get('/api/wiegand/readers', (req, res) => {
  if (!wiegandManager) {
    return res.status(503).json({ success: false, error: 'Wiegand system not initialized' });
  }
  res.json({ success: true, readers: wiegandManager.getReaders() });
});

app.get('/api/wiegand/readers/:readerId', (req, res) => {
  if (!wiegandManager) {
    return res.status(503).json({ success: false, error: 'Wiegand system not initialized' });
  }
  const reader = wiegandManager.getReader(req.params.readerId);
  if (!reader) {
    return res.status(404).json({ success: false, error: 'Reader not found' });
  }
  res.json({ success: true, reader });
});

app.post('/api/wiegand/send', async (req, res) => {
  if (!wiegandManager) {
    return res.status(503).json({ success: false, error: 'Wiegand system not initialized' });
  }
  try {
    const { door, readerId, facility, card, format } = req.body || {};
    const { readerId: targetReaderId, doorCfg } = resolveWiegandTarget({ door, readerId });

    if (facility === undefined || card === undefined) {
      return res.status(400).json({ success: false, error: 'facility and card required' });
    }
    const fmt = format != null ? Number(String(format).replace(/^W/i, '')) : null;
    const fac = Number(facility);
    const crd = Number(card);
    if (!Number.isFinite(fac) || !Number.isFinite(crd)) {
      return res.status(400).json({ success: false, error: 'facility and card must be numbers' });
    }

    const label = doorCfg ? `${doorCfg.name}` : `Reader ${targetReaderId}`;
    console.log(`[Wiegand] Sending credential -> ${label}`);
    if (doorCfg) console.log(`  GPIO: D0=${doorCfg.d0}, D1=${doorCfg.d1}`);
    console.log(`  Format: ${fmt ? `W${fmt}` : '(auto/default)'}`);
    console.log(`  Facility: ${fac}  Card: ${crd}`);

    const result = await wiegandManager.sendCard(targetReaderId, fac, crd, fmt);

    try {
      logAccessEvent && logAccessEvent(`Door ${doorCfg ? doorCfg.door : targetReaderId} unlocked by Wiegand card ${crd}`, {
        door: doorCfg ? doorCfg.door : null,
        readerId: targetReaderId,
        facility: fac,
        card: crd,
        method: 'wiegand'
      });
    } catch (e) {}

    return res.json({
      success: true,
      message: 'Credential sent',
      readerId: targetReaderId,
      door: doorCfg ? doorCfg.name : null,
      pins: doorCfg ? { d0: doorCfg.d0, d1: doorCfg.d1 } : null,
      ...result
    });
  } catch (err) {
    console.error('[Wiegand] Send error:', err && err.message ? err.message : err);
    return res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/wiegand/raw', async (req, res) => {
  if (!wiegandManager) {
    return res.status(503).json({ success: false, error: 'Wiegand system not initialized' });
  }
  try {
    const { readerId, bits } = req.body;
    if (!readerId || !bits) {
      return res.status(400).json({ success: false, error: 'readerId and bits required' });
    }
    const result = await wiegandManager.sendRaw(readerId, bits);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[Wiegand] Raw send error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/wiegand/test/:readerId', async (req, res) => {
  if (!wiegandManager) {
    return res.status(503).json({ success: false, error: 'Wiegand system not initialized' });
  }
  try {
    const result = await wiegandManager.testReader(req.params.readerId);
    res.json({ success: true, message: 'Test transmission successful', ...result });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

// ==================================================
// Native Wiegand transmitter endpoints
// ==================================================
app.post('/api/wiegand/transmit', (req, res) => {
  const { d0Pin, d1Pin, facility, card, bits = 26, pulseWidth = 50 } = req.body;

  if (d0Pin === undefined || d1Pin === undefined || facility === undefined || card === undefined) {
    return res.status(400).json({
      success: false,
      error: 'Missing required parameters: d0Pin, d1Pin, facility, card'
    });
  }

  const d0 = Number(d0Pin), d1 = Number(d1Pin);
  if (!Number.isInteger(d0) || !Number.isInteger(d1) || d0 < 0 || d0 > 27 || d1 < 0 || d1 > 27 || d0 === d1) {
    return res.status(400).json({
      success: false,
      error: 'Invalid GPIO pins. Must be integers 0-27 and different from each other'
    });
  }

  const bitsNum = Number(bits);
  const supportedFormats = [26, 30, 32, 34, 35, 37, 38, 40, 46, 48, 56, 64];
  if (!supportedFormats.includes(bitsNum)) {
    return res.status(400).json({
      success: false,
      error: `Invalid Wiegand format. Supported: ${supportedFormats.join(', ')} bits`
    });
  }

  if (bitsNum === 26) {
    if (facility < 0 || facility > 255) {
      return res.status(400).json({ success: false, error: '26-bit format: facility must be 0-255' });
    }
    if (card < 0 || card > 65535) {
      return res.status(400).json({ success: false, error: '26-bit format: card must be 0-65535' });
    }
  }

  const binPath = RESOLVED_WIEGAND_TX_PATH;
  if (!fs.existsSync(binPath)) {
    return res.status(500).json({
      success: false,
      error: `Wiegand transmitter not found at ${binPath}. Please compile it first.`
    });
  }

  const args = [String(d0), String(d1), String(facility), String(card), String(bitsNum), String(pulseWidth)];
  console.log(`[WIEGAND-NATIVE] Command: ${binPath} ${args.join(' ')}`);
  const startTime = Date.now();

  const p = spawn(binPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = '';
  p.stdout.on('data', d => {
    const output = d.toString();
    stdout += output;
    const lines = output.split('\n');
    lines.forEach(line => {
      if (line.trim()) {
        console.log(`[WIEGAND-NATIVE] ${line.trim()}`);
      }
    });
    const binaryMatch = output.match(/Binary: ([01]+) \((\d+) bits\)/);
    if (binaryMatch) {
      const binary = binaryMatch[1];
      const bits = binaryMatch[2];
      console.log(`[WIEGAND-BINARY] ${bits}-bit: ${binary}`);
    }
  });
  p.stderr.on('data', d => { stderr += d.toString(); });

  p.on('error', (err) => {
    const duration = Date.now() - startTime;
    const result = {
      timestamp: new Date().toISOString(), facility, card, bits: bitsNum, d0Pin: d0, d1Pin: d1, pulseWidth, duration,
      success: false, output: stdout, error: err.message
    };
    wiegandHistory.unshift(result);
    if (wiegandHistory.length > MAX_HISTORY) wiegandHistory.pop();
    console.error('[WIEGAND-NATIVE] Spawn error:', err.message);
    return res.status(500).json({ success: false, error: err.message, result });
  });

  p.on('exit', (code) => {
    const duration = Date.now() - startTime;
    const success = code === 0;
    const result = {
      timestamp: new Date().toISOString(), facility, card, bits: bitsNum, d0Pin: d0, d1Pin: d1, pulseWidth, duration,
      success, output: stdout, error: success ? null : (stderr || `exit ${code}`)
    };
    wiegandHistory.unshift(result);
    if (wiegandHistory.length > MAX_HISTORY) wiegandHistory.pop();

    if (!success) {
      console.error('[WIEGAND-NATIVE] Transmission failed:', result.error);
      return res.status(500).json({ success: false, error: result.error, result });
    }

    console.log(`[WIEGAND-NATIVE] Transmission successful (${duration}ms)`);

    try {
      logAccessEvent && logAccessEvent(`Native Wiegand TX - d0:${d0} d1:${d1} card:${card} bits:${bitsNum}`, {
        d0, d1, card, facility, bits: bitsNum, method: 'native'
      });
    } catch (e) {}

    res.json({ success: true, message: 'Wiegand transmission completed', result });
  });
});

app.get('/api/wiegand/history', (req, res) => {
  const limit = Math.min(100, Number(req.query.limit) || 50);
  res.json({ success: true, history: wiegandHistory.slice(0, limit), total: wiegandHistory.length });
});

app.delete('/api/wiegand/history', (req, res) => {
  wiegandHistory.length = 0;
  res.json({ success: true, message: 'History cleared' });
});

app.post('/api/wiegand/quick-test', (req, res, next) => {
  req.body = {
    d0Pin: 23,
    d1Pin: 24,
    facility: 123,
    card: 45678,
    bits: 26,
    pulseWidth: 50
  };
  return app._router.handle(req, res, next);
});

// ==================================================
// OSDP API ROUTES
// ==================================================
app.get('/api/osdp/status', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  res.json({ success: true, status: osdpManager.getStatus() });
});

app.get('/api/osdp/readers', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  res.json({ success: true, readers: osdpManager.getReaders() });
});

app.get('/api/osdp/readers/:readerId', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  const reader = osdpManager.getReader(req.params.readerId);
  if (!reader) {
    return res.status(404).json({ success: false, error: 'Reader not found' });
  }
  res.json({ success: true, reader });
});

app.post('/api/osdp/card-read', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const { readerId, facility, card, format } = req.body;
    if (!readerId || facility === undefined || card === undefined) {
      return res.status(400).json({ 
        success: false, 
        error: 'readerId, facility, and card required' 
      });
    }
    
    const cardData = { facility: Number(facility), card: Number(card) };
    const cardFormat = format || 'wiegand26';
    
    const result = await osdpManager.sendCardRead(readerId, cardData, cardFormat);

    try {
      logAccessEvent && logAccessEvent(`OSDP card-read API used - reader ${readerId} card ${card}`, {
        readerId, facility: cardData.facility, card: cardData.card, method: 'osdp_api'
      });
    } catch (e) {}

    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP] Card read error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/keyset', async (req, res) => {
  try {
    if (!osdpManager) return res.status(503).json({ success: false, error: 'OSDP not initialized' });
    const { readerId, keyHex } = req.body || {};
    if (!readerId || !keyHex) return res.status(400).json({ success: false, error: 'readerId and keyHex required' });

    if (typeof keyHex !== 'string' || !/^[0-9a-fA-F]+$/.test(keyHex) || (keyHex.length !== 32 && keyHex.length !== 64)) {
      return res.status(400).json({ success: false, error: 'keyHex must be 32 (128-bit) or 64 hex chars' });
    }

    if (typeof osdpManager.applyKeyset === 'function') {
      await osdpManager.applyKeyset(readerId, Buffer.from(keyHex, 'hex'));
      try { logSystemEvent && logSystemEvent(`Applied keyset to reader ${readerId}`, 'info'); } catch (e) {}
      return res.json({ success: true, message: 'KEYSET applied' });
    } else {
      return res.status(501).json({ success: false, error: 'osdpManager.applyKeyset not implemented' });
    }
  } catch (err) {
    console.error('[OSDP API] keyset error:', err && err.message ? err.message : err);
    res.status(500).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/led', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const { readerId, color, state, duration } = req.body;
    if (!readerId || !color || !state) {
      return res.status(400).json({ 
        success: false, 
        error: 'readerId, color, and state required' 
      });
    }
    
    const result = await osdpManager.setLED(
      readerId, 
      color, 
      state, 
      duration ? Number(duration) : 0
    );

    try { logSystemEvent && logSystemEvent(`OSDP LED ${state} for reader ${readerId} color ${color}`, 'info'); } catch (e) {}

    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP] LED control error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/buzzer', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const { readerId, duration } = req.body;
    if (!readerId) {
      return res.status(400).json({ success: false, error: 'readerId required' });
    }
    
    const result = await osdpManager.buzz(readerId, duration ? Number(duration) : 200);
    try { logSystemEvent && logSystemEvent(`OSDP buzzer triggered for reader ${readerId}`, 'info'); } catch (e) {}
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP] Buzzer control error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/keypad', async (req, res) => {
  console.log('[OSDP Route] POST /api/osdp/keypad called');
  console.log('[OSDP Route] Body:', JSON.stringify(req.body));

  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }

  try {
    const { readerId, data, format, facilityCode } = req.body;

    if (!readerId) {
      return res.status(400).json({ success: false, error: 'readerId required' });
    }
    if (!data) {
      return res.status(400).json({ success: false, error: 'keypad data required' });
    }

    const wiegandFormat = format || '8bit';
    const fc = facilityCode ? parseInt(facilityCode, 10) : 0;

    console.log(`[OSDP Route] Calling sendKeypadData: readerId=${readerId}, data=${data}, format=${wiegandFormat}, fc=${fc}`);
    const result = await osdpManager.sendKeypadData(readerId, data, wiegandFormat, fc);

    try {
      logAccessEvent && logAccessEvent(`OSDP keypad data sent to reader ${readerId}`, { readerId, dataLength: String(data).length, method: 'keypad' });
    } catch (e) {}

    console.log('[OSDP Route] sendKeypadData result:', JSON.stringify(result));
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP Route] ERROR:', err && err.message ? err.message : err);
    console.error('[OSDP Route] Stack:', err && err.stack ? err.stack : '');
    try {
      logSecurityEvent && logSecurityEvent('OSDP keypad route error', 'warning', { error: err && err.message ? err.message : String(err) });
    } catch (e) {}
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/test/:readerId', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = await osdpManager.testReader(req.params.readerId);
    res.json({ success: true, message: 'Reader test successful', ...result });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.put('/api/osdp/readers/:readerId', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = await osdpManager.updateReader(req.params.readerId, req.body);
    res.json({ success: true, reader: result });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.patch('/api/osdp/reader/:id', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = await osdpManager.updateReader(req.params.id, req.body);
    res.json({ success: true, reader: result });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/readers', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = await osdpManager.addReader(req.body);
    res.json({ success: true, reader: result });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.delete('/api/osdp/readers/:readerId', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    await osdpManager.removeReader(req.params.readerId);
    res.json({ success: true });
  } catch (err) {
    res.status(404).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.get('/api/osdp/stats', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  res.json({ success: true, stats: osdpManager.getStats() });
});

app.get('/api/osdp/formats', (req, res) => {
  if (!osdpManager) {
    const fallback = [
      { id: 'wiegand26', label: 'Wiegand 26-bit (8/16 + parity)', bitCount: 26 },
      { id: 'wiegand34', label: 'Wiegand 34-bit (16/16 + parity)', bitCount: 34 },
      { id: 'wiegand30', label: 'Wiegand 30-bit', bitCount: 30 },
      { id: 'wiegand32', label: 'Wiegand 32-bit', bitCount: 32 },
      { id: 'wiegand35', label: 'Wiegand 35-bit', bitCount: 35 },
      { id: 'wiegand37', label: 'Wiegand 37-bit', bitCount: 37 },
      { id: 'wiegand40', label: 'Wiegand 40-bit', bitCount: 40 },
      { id: 'wiegand48', label: 'Wiegand 48-bit', bitCount: 48 },
      { id: 'wiegand56', label: 'Wiegand 56-bit', bitCount: 56 },
      { id: 'wiegand64', label: 'Wiegand 64-bit', bitCount: 64 },
    ];
    return res.json({ success: true, formats: fallback });
  }

  try {
    const formats = (typeof osdpManager.getAvailableFormats === 'function')
      ? osdpManager.getAvailableFormats()
      : [
          { id: 'wiegand26', label: 'Wiegand 26-bit (8/16 + parity)', bitCount: 26 },
          { id: 'wiegand34', label: 'Wiegand 34-bit (16/16 + parity)', bitCount: 34 },
          { id: 'wiegand30', label: 'Wiegand 30-bit', bitCount: 30 },
          { id: 'wiegand32', label: 'Wiegand 32-bit', bitCount: 32 },
          { id: 'wiegand35', label: 'Wiegand 35-bit', bitCount: 35 },
          { id: 'wiegand37', label: 'Wiegand 37-bit', bitCount: 37 },
          { id: 'wiegand40', label: 'Wiegand 40-bit', bitCount: 40 },
          { id: 'wiegand48', label: 'Wiegand 48-bit', bitCount: 48 },
          { id: 'wiegand56', label: 'Wiegand 56-bit', bitCount: 56 },
          { id: 'wiegand64', label: 'Wiegand 64-bit', bitCount: 64 },
          { id: 'hid-h10301', label: 'HID H10301 (26-bit)', bitCount: 26 },
          { id: 'hid-h10302', label: 'HID H10302 (37-bit)', bitCount: 37 },
          { id: 'hid-h10304', label: 'HID H10304 (34-bit)', bitCount: 34 },
          { id: 'hid-corp1000-35', label: 'HID Corporate 1000 (35-bit)', bitCount: 35 },
          { id: 'hid-corp1000-48', label: 'HID Corporate 1000 (48-bit)', bitCount: 48 },
          { id: 'raw32', label: 'Raw 32-bit (no parity)', bitCount: 32 },
          { id: 'raw36', label: 'Raw 36-bit (no parity)', bitCount: 36 },
          { id: 'raw37', label: 'Raw 37-bit (no parity)', bitCount: 37 },
        ];

    res.json({ success: true, formats });
  } catch (e) {
    res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.get('/api/osdp/security', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const security = osdpManager.getSecurityInfo();
    res.json({ success: true, security });
  } catch (err) {
    console.error('[OSDP] Get security info error:', err && err.message ? err.message : err);
    res.status(500).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/security/keyset', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const { address, key } = req.body;
    if (address === undefined || !key) {
      return res.status(400).json({ success: false, error: 'address and key required' });
    }
    
    const result = await osdpManager.setCustomSCBK(address, key);
    
    try {
      logSecurityEvent && logSecurityEvent(`Custom SCBK set for reader at address 0x${address.toString(16)}`, 'info', { address });
    } catch (e) {}
    
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP] Set custom SCBK error:', err && err.message ? err.message : err);
    res.status(500).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/security/reset', async (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const { address } = req.body;
    if (address === undefined) {
      return res.status(400).json({ success: false, error: 'address required' });
    }
    
    const result = await osdpManager.resetToDefaultKey(address);
    
    try {
      logSecurityEvent && logSecurityEvent(`Reset to SCBK-D for reader at address 0x${address.toString(16)}`, 'info', { address });
    } catch (e) {}
    
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('[OSDP] Reset to default key error:', err && err.message ? err.message : err);
    res.status(500).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/osdp/capture/enable', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = osdpManager.enableCaptureMode();
    console.log('[OSDP] Packet capture enabled');
    return res.json(result);
  } catch (e) {
    console.error('[OSDP] Enable capture error:', e && e.message ? e.message : e);
    return res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.post('/api/osdp/capture/disable', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = osdpManager.disableCaptureMode();
    console.log('[OSDP] Packet capture disabled');
    return res.json(result);
  } catch (e) {
    console.error('[OSDP] Disable capture error:', e && e.message ? e.message : e);
    return res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.get('/api/osdp/capture/packets', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = osdpManager.getCapturedPackets();
    return res.json({ success: true, ...result });
  } catch (e) {
    console.error('[OSDP] Get captured packets error:', e && e.message ? e.message : e);
    return res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

app.post('/api/osdp/capture/clear', (req, res) => {
  if (!osdpManager) {
    return res.status(503).json({ success: false, error: 'OSDP system not initialized' });
  }
  try {
    const result = osdpManager.clearCapturedPackets();
    console.log('[OSDP] Captured packets cleared');
    return res.json(result);
  } catch (e) {
    console.error('[OSDP] Clear captured packets error:', e && e.message ? e.message : e);
    return res.status(500).json({ success: false, error: e && e.message ? e.message : String(e) });
  }
});

// ==================================================
// NFC API ROUTES
// ==================================================
app.get('/api/nfc/status', (req, res) => {
  if (!nfcManager) {
    return res.json({ success: true, enabled: false, error: 'NFC Manager not initialized', status: null });
  }
  const status = nfcManager.getStatus();
  res.json({ success: true, enabled: nfcManager.enabled, status });
});

app.post('/api/nfc/start', async (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  try {
    await nfcManager.startReading();
    res.json({ success: true, message: 'NFC reading started' });
  } catch (err) {
    console.error('[NFC API] Start error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/nfc/stop', (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  try {
    nfcManager.stopReading();
    res.json({ success: true, message: 'NFC reading stopped' });
  } catch (err) {
    console.error('[NFC API] Stop error:', err && err.message ? err.message : err);
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.get('/api/nfc/config', (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  res.json({ success: true, config: nfcManager.config });
});

app.get('/api/nfc/bridge/status', (req, res) => {
  if (!nfcBridge) {
    return res.json({ success: true, available: false, enabled: false, message: 'Bridge not initialized' });
  }
  const stats = nfcBridge.getStats();
  res.json({ success: true, available: true, ...stats });
});

app.post('/api/nfc/bridge/enable', (req, res) => {
  if (!nfcBridge) {
    return res.status(503).json({ success: false, error: 'Bridge not initialized' });
  }
  try {
    nfcBridge.enable();
    res.json({ success: true, message: 'Bridge enabled' });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/nfc/bridge/disable', (req, res) => {
  if (!nfcBridge) {
    return res.status(503).json({ success: false, error: 'Bridge not initialized' });
  }
  try {
    nfcBridge.disable();
    res.json({ success: true, message: 'Bridge disabled' });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.post('/api/nfc/bridge/config', (req, res) => {
  if (!nfcBridge) {
    return res.status(503).json({ success: false, error: 'Bridge not initialized' });
  }
  try {
    const { defaultReaderId, defaultFormat, defaultFacilityCode } = req.body;
    nfcBridge.setConfig({
      defaultReaderId: defaultReaderId || nfcBridge.config.defaultReaderId,
      defaultFormat: defaultFormat || nfcBridge.config.defaultFormat,
      defaultFacilityCode: defaultFacilityCode !== undefined ? defaultFacilityCode : nfcBridge.config.defaultFacilityCode
    });
    res.json({ success: true, config: nfcBridge.getConfig() });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.get('/api/nfc/bridge/test', async (req, res) => {
  if (!nfcBridge) {
    return res.status(503).json({ success: false, error: 'Bridge not initialized' });
  }
  try {
    const result = await nfcBridge.testConnection();
    res.json({ success: true, connected: result, message: result ? 'Bridge connection OK' : 'Bridge connection failed' });
  } catch (err) {
    res.status(400).json({ success: false, error: err && err.message ? err.message : String(err) });
  }
});

app.get('/api/nfc/bridge/stats', (req, res) => {
  if (!nfcBridge) {
    return res.json({ success: true, stats: null });
  }
  res.json({ success: true, stats: nfcBridge.getStats() });
});

// ---- NFC Polling Control (for new PN532Manager) ----
app.post('/api/nfc/polling/start', (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  
  // Support both old and new API
  const startFn = nfcManager.startPolling || nfcManager.startReading;
  if (typeof startFn !== 'function') {
    return res.status(400).json({ success: false, error: 'Polling not supported by this NFC manager' });
  }
  
  try {
    const result = startFn.call(nfcManager);
    console.log('[NFC API] Polling started');
    res.json({ success: true, message: 'NFC polling started', result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post('/api/nfc/polling/stop', (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  
  // Support both old and new API
  const stopFn = nfcManager.stopPolling || nfcManager.stopReading;
  if (typeof stopFn !== 'function') {
    return res.status(400).json({ success: false, error: 'Polling control not supported' });
  }
  
  try {
    const result = stopFn.call(nfcManager);
    console.log('[NFC API] Polling stopped');
    res.json({ success: true, message: 'NFC polling stopped', result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.get('/api/nfc/polling/status', (req, res) => {
  if (!nfcManager) {
    return res.json({ success: true, polling: false, error: 'NFC Manager not initialized' });
  }
  
  const isPolling = nfcManager.isPolling ? nfcManager.isPolling() : nfcManager.shouldPoll || false;
  const status = nfcManager.getStatus ? nfcManager.getStatus() : {};
  
  res.json({ 
    success: true, 
    polling: isPolling,
    ...status
  });
});

app.post('/api/nfc/polling/config', (req, res) => {
  if (!nfcManager) {
    return res.status(503).json({ success: false, error: 'NFC Manager not initialized' });
  }
  
  if (typeof nfcManager.setConfig !== 'function') {
    return res.status(400).json({ success: false, error: 'Config not supported' });
  }
  
  try {
    const { pollInterval, pollTimeout, maxConsecutiveErrors } = req.body;
    const newConfig = nfcManager.setConfig({ pollInterval, pollTimeout, maxConsecutiveErrors });
    res.json({ success: true, config: newConfig });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

// ---- Socket.IO ----
io.on('connection', (socket) => {
  console.log('[Socket.IO] Client connected:', socket.id);
  const s = {}; 
  states.forEach((v, p) => s[p] = v);
  socket.emit('initial_states', s);
  
  socket.on('set_gpio', ({ pin, value, pulseMs }) => {
    if (Number.isInteger(pulseMs) && pulseMs > 0) {
      pulse(pin, pulseMs).catch(err => console.error('[GPIO] Pulse error:', err && err.message ? err.message : err));
    } else {
      try {
        holdLevel(pin, value ? 1 : 0);
        io.emit('gpio_state_change', { pin, value: value ? 1 : 0 });
      } catch (e) {
        console.error('[Socket.IO] GPIO set blocked:', e && e.message ? e.message : e);
      }
    }
  });

  socket.on('nfc:getStatus', () => {
    if (nfcManager) {
      socket.emit('nfc:status', nfcManager.getStatus());
    } else {
      socket.emit('nfc:status', { enabled: false, error: 'NFC not available' });
    }
  });

  socket.on('nfc:start', async () => {
    if (nfcManager) {
      try {
        await nfcManager.startReading();
        socket.emit('nfc:started');
      } catch (err) {
        socket.emit('nfc:error', { error: err && err.message ? err.message : String(err) });
      }
    } else {
      socket.emit('nfc:error', { error: 'NFC not available' });
    }
  });

  socket.on('nfc:stop', () => {
    if (nfcManager) {
      nfcManager.stopReading();
      socket.emit('nfc:stopped');
    } else {
      socket.emit('nfc:error', { error: 'NFC not available' });
    }
  });

  socket.on('nfc:bridge:getStatus', () => {
    if (nfcBridge) {
      socket.emit('nfc:bridge:status', nfcBridge.getStats());
    } else {
      socket.emit('nfc:bridge:status', { available: false });
    }
  });

  socket.on('nfc:bridge:enable', () => {
    if (nfcBridge) {
      nfcBridge.enable();
      socket.emit('nfc:bridge:enabled');
    } else {
      socket.emit('nfc:error', { error: 'Bridge not available' });
    }
  });

  socket.on('nfc:bridge:disable', () => {
    if (nfcBridge) {
      nfcBridge.disable();
      socket.emit('nfc:bridge:disabled');
    } else {
      socket.emit('nfc:error', { error: 'Bridge not available' });
    }
  });
  
  socket.on('disconnect', () => {
    console.log('[Socket.IO] Client disconnected:', socket.id);
  });
});

// ---- Cleanup ----
process.on('SIGINT', async () => {
  console.log('\n[Cleanup] Cleaning up gpioset holders...');
  procs.forEach((p) => { 
    try { process.kill(p.pid, 'SIGKILL'); } catch (e) {} 
  });

  // Stop input monitor
  if (inputMonitor) {
    inputMonitor.stop();
    console.log('[InputMonitor] Stopped');
  }

  if (nfcManager && typeof nfcManager.close === 'function') {
    try {
      await nfcManager.close();
      console.log('[NFC] Closed');
    } catch (err) {
      console.error('[NFC] Cleanup error:', err && err.message ? err.message : err);
    }
  }

  gpioQueue.destroy();
  console.log('[Queue] Destroyed');

  try { logSystemEvent && logSystemEvent('System shutting down', 'info'); } catch (e) {}

  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('[Server] SIGTERM received, cleaning up...');
  if (inputMonitor) {
    inputMonitor.stop();
  }
  gpioQueue.destroy();
  process.exit(0);
});

// ---- Start Server ----
const PORT = process.env.PORT || 3001;
server.listen(PORT, '0.0.0.0', () => {
  const services = {
    'Automation': createServiceInfo(!!automationManager, automationManager ? {
      'Rules': typeof automationManager.getRules === 'function' ? automationManager.getRules().length : undefined,
      'Active': automationManager?.activeRules ?? undefined
    } : null),
    
    'Wiegand': createServiceInfo(!!wiegandManager, (wiegandManager ? {
      'Reserved Pins': Array.from(wiegandManager.reservedPins || []).join(', '),
      'Readers': typeof wiegandManager.getReaders === 'function' ? wiegandManager.getReaders().length : undefined
    } : null)),
    
    'OSDP': createServiceInfo(!!osdpManager, (osdpManager ? {
      'Readers': osdpManager.readers ? (osdpManager.readers.size || Object.keys(osdpManager.readers).length) : 0,
      'Port': osdpManager.config?.serialPort || 'N/A'
    } : null)),
    
    'NFC': createServiceInfo(!!nfcManager && !!nfcManager.enabled, (nfcManager && nfcManager.enabled ? {
      'Device': nfcManager.deviceType || 'PN532',
      'Mode': nfcManager.mode || 'Reader'
    } : null)),
    
    'Bridge': createServiceInfo(!!nfcBridge && typeof nfcBridge.isEnabled === 'function' && nfcBridge.isEnabled(), (nfcBridge ? {
      'Mode': nfcBridge.mode || 'Auto',
      'Status': 'Active'
    } : null)),
    
    'Native TX': createServiceInfo(fs.existsSync(RESOLVED_WIEGAND_TX_PATH), fs.existsSync(RESOLVED_WIEGAND_TX_PATH) ? {
      'Path': RESOLVED_WIEGAND_TX_PATH,
      'Version': '2.0'
    } : null),
    
    'Format API': createServiceInfo(true, {
      'Formats': (typeof (formatRoutes && formatRoutes.getFormatsCount) === 'function') ? formatRoutes.getFormatsCount() : 'standard',
      'Custom': (typeof (formatRoutes && formatRoutes.getCustomFormatsCount) === 'function') ? formatRoutes.getCustomFormatsCount() : 0
    }),

    'GPIO Queue': createServiceInfo(true, {
      'Max Burst': gpioQueue.config.maxBurstCommands + '/sec',
      'Queue Size': gpioQueue.config.maxQueueSize,
      'Min Delay': gpioQueue.config.minDelayBetweenCommands + 'ms'
    }),

    'Input Monitor': createServiceInfo(!!inputMonitor && inputMonitor.enabled, inputMonitor ? {
      'Poll Interval': inputMonitor.config.pollIntervalMs + 'ms',
      'Opto Enabled': inputMonitor.config.enableOpto ? 'Yes' : 'No',
      'Analog Enabled': inputMonitor.config.enableAnalog ? 'Yes' : 'No',
      'Analog Threshold': inputMonitor.config.analogThreshold + 'mV'
    } : null)
  };

  const startupInfo = {
    title: 'GPIO Control Server (libgpiod) + Queue Protection',
    version: '2.1',
    chip: CHIP,
    port: PORT,
    binding: '0.0.0.0',
    services,
    buildInfo: process.env.BUILD_INFO || null,
    timestamp: new Date().toISOString()
  };

  if (logger && typeof logger.log === 'function') {
    logger.log(startupInfo);
  } else {
    console.log('--- GPIO Control Server ---');
    console.log('Version:', startupInfo.version);
    console.log('Chip:', startupInfo.chip);
    console.log('Port:', startupInfo.port);
    console.log('Binding:', startupInfo.binding);
    console.log('Services:', Object.keys(services).map(k => `${k}: ${services[k].ok ? 'ENABLED' : 'DISABLED'}`).join(', '));
    console.log('Ready to accept connections');
  }

  try {
    logSystemEvent && logSystemEvent('System started with GPIO queue protection', 'info');
  } catch (e) {}

  logServerStatus();
  
  // ═══════════════════════════════════════════════════════════════════════
  // DELAYED INPUT MONITOR AUTO-START
  // ═══════════════════════════════════════════════════════════════════════
  // Start InputMonitor 10 seconds after server is ready to avoid I2C
  // bus contention during OSDP/Wiegand/NFC initialization
  setTimeout(() => {
    console.log('[InputMonitor] Delayed auto-start initiating...');
    try {
      inputMonitor.start();
      console.log('[InputMonitor] ✓ Auto-started after 10s delay');
    } catch (err) {
      console.error('[InputMonitor] Failed to auto-start:', err.message);
    }
  }, 10000);  // 10 second delay
});

module.exports = { app, server, logger: logger || console, logServerStatus };
