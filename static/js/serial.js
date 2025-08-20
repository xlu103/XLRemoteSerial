class WebSerialManager {
    constructor() {
        this.port = null;
        this.reader = null;
        this.writer = null;
        this.isConnected = false;
        this.onDataReceived = null;
        this.onConnectionChange = null;
        this.readLoop = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.debugMode = false; // 调试模式开关
        this.bufferSize = 0; // 缓冲区大小统计
    }

    // 检查浏览器是否支持Web Serial API
    isSupported() {
        return 'serial' in navigator;
    }

    // 获取可用串口列表
    async getPorts() {
        if (!this.isSupported()) {
            throw new Error('浏览器不支持Web Serial API');
        }
        return await navigator.serial.getPorts();
    }

    // 请求串口权限并连接
    async connect(options = {}) {
        if (!this.isSupported()) {
            throw new Error('浏览器不支持Web Serial API');
        }

        try {
            // 请求串口权限
            this.port = await navigator.serial.requestPort();
            
            // 配置串口参数
            const config = {
                baudRate: options.baudrate || 115200,
                dataBits: options.bytesize || 8,
                stopBits: options.stopbits || 1,
                parity: this.mapParity(options.parity),
                bufferSize: 1024,
                flowControl: 'none'
            };

            await this.port.open(config);
            
            // 获取读写器
            this.writer = this.port.writable.getWriter();
            this.reader = this.port.readable.getReader();
            
            this.isConnected = true;
            this.reconnectAttempts = 0;
            
            // 开始读取数据
            this.startReading();
            
            // 通知连接状态变化
            if (this.onConnectionChange) {
                this.onConnectionChange(true);
            }
            
            return true;
        } catch (error) {
            console.error('串口连接失败:', error);
            this.isConnected = false;
            throw error;
        }
    }

    // 断开连接
    async disconnect() {
        try {
            if (this.readLoop) {
                clearInterval(this.readLoop);
                this.readLoop = null;
            }
            
            if (this.reader) {
                await this.reader.cancel();
                this.reader.releaseLock();
                this.reader = null;
            }
            
            if (this.writer) {
                this.writer.releaseLock();
                this.writer = null;
            }
            
            if (this.port) {
                await this.port.close();
                this.port = null;
            }
            
            this.isConnected = false;
            
            // 通知连接状态变化
            if (this.onConnectionChange) {
                this.onConnectionChange(false);
            }
        } catch (error) {
            console.error('断开连接失败:', error);
        }
    }

    // 开始读取数据
    async startReading() {
        if (!this.reader || !this.isConnected) return;

        try {
            // 创建文本解码器
            const decoder = new TextDecoder('utf-8', { fatal: false });
            let buffer = ''; // 数据缓冲区
            
            while (this.isConnected && this.port.readable) {
                const { value, done } = await this.reader.read();
                
                if (done) {
                    console.log('串口读取完成');
                    break;
                }
                
                if (value && this.onDataReceived) {
                    // 将Uint8Array转换为字符串
                    const chunk = decoder.decode(value, { stream: true });
                    buffer += chunk;
                    
                    // 处理缓冲区中的数据
                    buffer = this.processBuffer(buffer);
                }
            }
        } catch (error) {
            console.error('读取数据失败:', error);
            // 尝试重连
            this.handleConnectionError();
        }
    }

    // 处理数据缓冲区，避免数据分段
    processBuffer(buffer) {
        if (!buffer) return '';
        
        // 统一换行符处理
        let normalizedBuffer = buffer.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        
        // 按行分割数据
        const lines = normalizedBuffer.split('\n');
        
        // 调试信息（可选）
        if (this.debugMode && lines.length > 1) {
            console.log(`处理缓冲区: ${lines.length} 行, 缓冲区长度: ${buffer.length}`);
        }
        
        // 处理完整的行（除了最后一行）
        for (let i = 0; i < lines.length - 1; i++) {
            const line = lines[i];
            if (line !== '') {
                this.onDataReceived(line);
            }
        }
        
        // 检查最后一行是否完整
        const lastLine = lines[lines.length - 1];
        
        // 如果最后一行不为空且不以换行符结尾，说明可能不完整
        if (lastLine !== '' && !buffer.endsWith('\n') && !buffer.endsWith('\r')) {
            // 不完整的行，保留在缓冲区中
            if (this.debugMode) {
                console.log(`保留不完整行: "${lastLine}" (长度: ${lastLine.length})`);
            }
            return lastLine;
        } else if (lastLine !== '') {
            // 完整的行，发送并清空缓冲区
            this.onDataReceived(lastLine);
            return '';
        }
        
        // 缓冲区为空或已处理完
        return '';
    }

    // 写入数据
    async writeData(data, encoding = 'utf-8') {
        if (!this.writer || !this.isConnected) {
            throw new Error('串口未连接');
        }

        try {
            let bytes;
            if (typeof data === 'string') {
                bytes = new TextEncoder().encode(data);
            } else {
                bytes = new Uint8Array(data);
            }
            
            await this.writer.write(bytes);
            return true;
        } catch (error) {
            console.error('写入数据失败:', error);
            throw error;
        }
    }

    // 处理连接错误
    async handleConnectionError() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            console.log(`尝试重连... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
            
            // 等待一段时间后重连
            setTimeout(async () => {
                try {
                    await this.disconnect();
                    // 这里可以添加重连逻辑
                } catch (error) {
                    console.error('重连失败:', error);
                }
            }, 1000 * this.reconnectAttempts);
        } else {
            console.error('重连次数超过限制，停止重连');
        }
    }

    // 映射校验位值
    mapParity(parity) {
        const parityMap = {
            'none': 'none',
            'even': 'even', 
            'odd': 'odd',
            'N': 'none',
            'E': 'even',
            'O': 'odd'
        };
        return parityMap[parity] || 'none';
    }

    // 获取连接状态
    getConnectionStatus() {
        return {
            isConnected: this.isConnected,
            port: this.port ? this.port.getInfo() : null,
            reconnectAttempts: this.reconnectAttempts
        };
    }

    // 设置数据接收回调
    setDataCallback(callback) {
        this.onDataReceived = callback;
    }

    // 设置连接状态变化回调
    setConnectionCallback(callback) {
        this.onConnectionChange = callback;
    }

    // 发送数据到后端API
    async sendDataToBackend(deviceCode, data) {
        try {
            const response = await fetch('/api/serial/data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    device_code: deviceCode,
                    data: data
                })
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error('发送数据到后端失败:', error);
            throw error;
        }
    }

    // 获取设备码
    async getDeviceCode() {
        try {
            const response = await fetch('/api/device');
            const data = await response.json();
            return data.device_code;
        } catch (error) {
            console.error('获取设备码失败:', error);
            return null;
        }
    }

    // 获取串口信息
    getPortInfo() {
        if (this.port) {
            try {
                return this.port.getInfo();
            } catch (error) {
                console.error('获取串口信息失败:', error);
                return null;
            }
        }
        return null;
    }

    // 切换调试模式
    setDebugMode(enabled) {
        this.debugMode = enabled;
        console.log(`调试模式: ${enabled ? '开启' : '关闭'}`);
    }

    // 获取缓冲区统计信息
    getBufferStats() {
        return {
            debugMode: this.debugMode,
            bufferSize: this.bufferSize,
            isConnected: this.isConnected
        };
    }
}

// 全局串口管理器实例
window.serialManager = new WebSerialManager(); 