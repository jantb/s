// Chart functionality
let logLevelChart = null;
let kafkaLagChart = null;
let logClustersChart = null;
let chartTooltip = null;

// Initialize charts when the page loads
document.addEventListener('DOMContentLoaded', () => {
    chartTooltip = document.getElementById('chart-tooltip');
    initializeLogLevelChart();
});

// Log Level Chart implementation
function initializeLogLevelChart() {
    const canvas = document.getElementById('log-level-chart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    logLevelChart = {
        canvas: canvas,
        ctx: ctx,
        data: [],
        levels: ['INFO', 'WARN', 'ERROR', 'DEBUG', 'KAFKA'],
        levelColors: {
            'INFO': '#4CAF50',
            'WARN': '#FF9800',
            'ERROR': '#F44336',
            'DEBUG': '#607D8B',
            'KAFKA': '#00BCD4'
        },
        scaleMax: 100,
        hoveredBar: null,
        timeFormatter: new Intl.DateTimeFormat('en-US', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        })
    };
    
    // Set canvas size
    resizeCanvas(canvas);
    
    // Add event listeners
    canvas.addEventListener('mousemove', handleLogLevelChartMouseMove);
    canvas.addEventListener('mouseout', handleLogLevelChartMouseOut);
    canvas.addEventListener('click', handleLogLevelChartClick);
    
    // Draw initial empty chart
    drawLogLevelChart();
}

function resizeCanvas(canvas) {
    const displayWidth = canvas.clientWidth;
    const displayHeight = canvas.clientHeight;
    
    if (canvas.width !== displayWidth || canvas.height !== displayHeight) {
        canvas.width = displayWidth;
        canvas.height = displayHeight;
    }
}

function drawLogLevelChart() {
    if (!logLevelChart) return;
    
    const { ctx, canvas, data, levels, levelColors, scaleMax } = logLevelChart;
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // Set background
    ctx.fillStyle = '#252526';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    
    // If no data, show message
    if (data.length === 0) {
        ctx.fillStyle = '#d4d4d4';
        ctx.font = '14px JetBrains Mono';
        ctx.textAlign = 'center';
        ctx.fillText('No chart data available', canvas.width / 2, canvas.height / 2);
        return;
    }
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Draw grid lines and labels
    ctx.strokeStyle = '#3c3c3c';
    ctx.lineWidth = 1;
    ctx.font = '10px JetBrains Mono';
    ctx.textAlign = 'right';
    ctx.fillStyle = '#d4d4d4';
    
    // Y-axis grid lines and labels
    for (let i = 0; i <= 5; i++) {
        const y = margin.top + chartHeight - (i * chartHeight / 5);
        ctx.beginPath();
        ctx.moveTo(margin.left, y);
        ctx.lineTo(canvas.width - margin.right, y);
        ctx.stroke();
        
        const value = Math.round(scaleMax * i / 5);
        ctx.fillText(value.toString(), margin.left - 10, y + 4);
    }
    
    // X-axis labels
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    const timeSlotWidth = chartWidth / Math.max(1, data.length);
    
    for (let i = 0; i < data.length; i += Math.max(1, Math.floor(data.length / 10))) {
        const x = margin.left + (i * timeSlotWidth) + (timeSlotWidth / 2);
        const timeStr = logLevelChart.timeFormatter.format(data[i].time);
        ctx.fillText(timeStr, x, margin.top + chartHeight + 5);
    }
    
    // Draw bars
    const barWidth = Math.max(1, timeSlotWidth - 2);
    
    for (let i = 0; i < data.length; i++) {
        const x = margin.left + (i * timeSlotWidth);
        let currentHeight = 0;
        
        // Draw bars for each level (stacked)
        for (const level of levels) {
            if (activeLevels.has(level)) {
                const count = data[i].counts[level] || 0;
                if (count > 0) {
                    const barHeight = Math.max(1, (count / scaleMax) * chartHeight);
                    const y = margin.top + chartHeight - currentHeight - barHeight;
                    
                    // Draw bar
                    ctx.fillStyle = levelColors[level];
                    ctx.fillRect(x, y, barWidth, barHeight);
                    
                    currentHeight += barHeight;
                }
            }
        }
    }
    
    // Draw axes
    ctx.strokeStyle = '#d4d4d4';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(margin.left, margin.top);
    ctx.lineTo(margin.left, margin.top + chartHeight);
    ctx.lineTo(canvas.width - margin.right, margin.top + chartHeight);
    ctx.stroke();
    
    // Draw legend
    drawLogLevelChartLegend();
}

function drawLogLevelChartLegend() {
    if (!logLevelChart) return;
    
    const { ctx, canvas, levels, levelColors } = logLevelChart;
    const legendY = 15;
    const labelWidth = 80;
    const totalWidth = labelWidth * activeLevels.size;
    const startX = (canvas.width - totalWidth) / 2;
    
    ctx.font = '11px JetBrains Mono';
    ctx.textAlign = 'left';
    
    let index = 0;
    for (const level of levels) {
        if (activeLevels.has(level)) {
            const x = startX + (index * labelWidth);
            
            // Draw color box
            ctx.fillStyle = levelColors[level];
            ctx.fillRect(x, legendY - 8, 12, 12);
            
            // Draw level name
            ctx.fillStyle = '#d4d4d4';
            ctx.fillText(level, x + 18, legendY);
            
            index++;
        }
    }
}

function handleLogLevelChartMouseMove(e) {
    if (!logLevelChart || logLevelChart.data.length === 0) return;
    
    const { canvas, data, levels } = logLevelChart;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Check if mouse is over chart area
    if (x < margin.left || x > canvas.width - margin.right ||
        y < margin.top || y > margin.top + chartHeight) {
        logLevelChart.hoveredBar = null;
        hideChartTooltip();
        return;
    }
    
    // Calculate which time slot is hovered
    const timeSlotWidth = chartWidth / Math.max(1, data.length);
    const timeIndex = Math.floor((x - margin.left) / timeSlotWidth);
    
    if (timeIndex >= 0 && timeIndex < data.length) {
        const timePoint = data[timeIndex];
        const barX = margin.left + (timeIndex * timeSlotWidth);
        
        // Calculate which level bar is hovered
        let currentHeight = 0;
        let hoveredLevel = null;
        let levelHeight = 0;
        
        // Go through levels in reverse order (top to bottom)
        for (const level of [...levels].reverse()) {
            if (activeLevels.has(level)) {
                const count = timePoint.counts[level] || 0;
                if (count > 0) {
                    const barHeight = Math.max(1, (count / logLevelChart.scaleMax) * chartHeight);
                    const barY = margin.top + chartHeight - currentHeight - barHeight;
                    
                    // Check if mouse is over this bar
                    if (y >= barY && y <= barY + barHeight) {
                        hoveredLevel = level;
                        levelHeight = barHeight;
                        break;
                    }
                    
                    currentHeight += barHeight;
                }
            }
        }
        
        if (hoveredLevel) {
            logLevelChart.hoveredBar = { timeIndex, level: hoveredLevel };
            showLogLevelChartTooltip(timePoint, hoveredLevel, x, y);
        } else {
            logLevelChart.hoveredBar = null;
            hideChartTooltip();
        }
    } else {
        logLevelChart.hoveredBar = null;
        hideChartTooltip();
    }
}

function handleLogLevelChartMouseOut() {
    if (logLevelChart) {
        logLevelChart.hoveredBar = null;
        hideChartTooltip();
    }
}

function handleLogLevelChartClick(e) {
    if (!logLevelChart || !logLevelChart.hoveredBar) return;
    
    // Handle click-to-navigate functionality
    // This would typically update the log view to show logs from the selected time period
    console.log('Chart bar clicked:', logLevelChart.hoveredBar);
}

function showLogLevelChartTooltip(timePoint, level, x, y) {
    if (!chartTooltip) return;
    
    const count = timePoint.counts[level] || 0;
    const total = Object.values(timePoint.counts).reduce((sum, val) => sum + val, 0);
    const percentage = total > 0 ? ((count / total) * 100).toFixed(1) : '0.0';
    
    const timeStr = logLevelChart.timeFormatter.format(timePoint.time);
    
    chartTooltip.innerHTML = `
        <div class="chart-tooltip-header">${timeStr}</div>
        <div class="chart-tooltip-item">
            <span style="color: ${logLevelChart.levelColors[level]}">${level}:</span>
            <span class="chart-tooltip-value">${count}</span>
        </div>
        <div class="chart-tooltip-item">
            <span>Percentage:</span>
            <span class="chart-tooltip-value">${percentage}%</span>
        </div>
        <div class="chart-tooltip-item">
            <span>Total:</span>
            <span class="chart-tooltip-value">${total}</span>
        </div>
    `;
    
    // Position tooltip closer to mouse
    chartTooltip.style.display = 'block';
    chartTooltip.style.left = (x + 10) + 'px';
    chartTooltip.style.top = (y + 10) + 'px';
}

function hideChartTooltip() {
    if (chartTooltip) {
        chartTooltip.style.display = 'none';
    }
}

// Update the log level chart with new data
function updateLogLevelChart(chartData) {
    if (!logLevelChart) {
        console.log('Log level chart not initialized, initializing now...');
        initializeLogLevelChart();
        if (!logLevelChart) {
            console.error('Failed to initialize log level chart');
            return;
        }
    }
    
    console.log('Updating log level chart with data:', chartData);
    
    // Convert chart data to our format
    logLevelChart.data = chartData.timePoints.map(point => ({
        time: new Date(point.time),
        counts: {}
    }));
    
    // Populate counts for each level
    chartData.timePoints.forEach((point, index) => {
        chartData.levels.forEach((level, levelIndex) => {
            logLevelChart.data[index].counts[level] = point.counts[levelIndex] || 0;
        });
    });
    
    logLevelChart.scaleMax = Math.max(chartData.scaleMax, 1);
    
    console.log('Chart data processed, redrawing chart...');
    
    // Redraw chart
    drawLogLevelChart();
}

// Kafka Lag Chart implementation
function initializeKafkaLagChart() {
    const canvas = document.getElementById('kafka-lag-chart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    kafkaLagChart = {
        canvas: canvas,
        ctx: ctx,
        data: [],
        hideZeroLag: false,
        hoveredBar: null
    };
    
    // Set canvas size
    resizeCanvas(canvas);
    
    // Add event listeners
    canvas.addEventListener('mousemove', handleKafkaLagChartMouseMove);
    canvas.addEventListener('mouseout', handleKafkaLagChartMouseOut);
    
    // Draw initial empty chart
    drawKafkaLagChart();
}

function drawKafkaLagChart() {
    if (!kafkaLagChart) return;
    
    const { ctx, canvas, data, hideZeroLag } = kafkaLagChart;
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // Set background
    ctx.fillStyle = '#252526';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    
    // Filter data if needed
    const filteredData = hideZeroLag ? data.filter(item => item.lag > 0) : data;
    
    // If no data, show message
    if (filteredData.length === 0) {
        ctx.fillStyle = '#d4d4d4';
        ctx.font = '14px JetBrains Mono';
        ctx.textAlign = 'center';
        ctx.fillText('No lag data available', canvas.width / 2, canvas.height / 2);
        return;
    }
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Find max lag for scaling
    const maxLag = Math.max(...filteredData.map(item => item.lag), 1);
    
    // Draw bars
    const barWidth = Math.max(2, chartWidth / Math.max(1, filteredData.length) - 4);
    const barSpacing = 4;
    
    ctx.font = '10px JetBrains Mono';
    ctx.textAlign = 'center';
    
    for (let i = 0; i < filteredData.length; i++) {
        const item = filteredData[i];
        const x = margin.left + (i * (barWidth + barSpacing));
        const barHeight = Math.max(1, (item.lag / maxLag) * chartHeight);
        const y = margin.top + chartHeight - barHeight;
        
        // Draw bar
        ctx.fillStyle = '#F44336'; // Red color for lag
        ctx.fillRect(x, y, barWidth, barHeight);
        
        // Draw topic name (truncated if too long)
        const topicLabel = item.topic.length > 15 ? item.topic.substring(0, 12) + '...' : item.topic;
        ctx.fillStyle = '#d4d4d4';
        ctx.fillText(topicLabel, x + barWidth / 2, margin.top + chartHeight + 15);
        
        // Draw lag value
        ctx.fillText(item.lag.toString(), x + barWidth / 2, y - 5);
    }
    
    // Draw axes
    ctx.strokeStyle = '#d4d4d4';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(margin.left, margin.top);
    ctx.lineTo(margin.left, margin.top + chartHeight);
    ctx.lineTo(canvas.width - margin.right, margin.top + chartHeight);
    ctx.stroke();
    
    // Draw title
    ctx.fillStyle = '#d4d4d4';
    ctx.font = '12px JetBrains Mono';
    ctx.textAlign = 'left';
    ctx.fillText('Kafka Consumer Lag by Topic', margin.left, margin.top - 10);
}

function handleKafkaLagChartMouseMove(e) {
    if (!kafkaLagChart || kafkaLagChart.data.length === 0) return;
    
    const { canvas, data } = kafkaLagChart;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Check if mouse is over chart area
    if (x < margin.left || x > canvas.width - margin.right ||
        y < margin.top || y > margin.top + chartHeight) {
        kafkaLagChart.hoveredBar = null;
        hideChartTooltip();
        return;
    }
    
    // Find which bar is hovered
    const filteredData = kafkaLagChart.hideZeroLag ? data.filter(item => item.lag > 0) : data;
    const barWidth = Math.max(2, chartWidth / Math.max(1, filteredData.length) - 4);
    const barSpacing = 4;
    
    for (let i = 0; i < filteredData.length; i++) {
        const item = filteredData[i];
        const barX = margin.left + (i * (barWidth + barSpacing));
        
        // Check if mouse is over this bar
        if (x >= barX && x <= barX + barWidth) {
            kafkaLagChart.hoveredBar = i;
            showKafkaLagChartTooltip(item, x, y);
            return;
        }
    }
    
    kafkaLagChart.hoveredBar = null;
    hideChartTooltip();
}

function handleKafkaLagChartMouseOut() {
    if (kafkaLagChart) {
        kafkaLagChart.hoveredBar = null;
        hideChartTooltip();
    }
}

function showKafkaLagChartTooltip(item, x, y) {
    if (!chartTooltip) return;
    
    chartTooltip.innerHTML = `
        <div class="chart-tooltip-header">${item.topic}</div>
        <div class="chart-tooltip-item">
            <span>Lag:</span>
            <span class="chart-tooltip-value">${item.lag}</span>
        </div>
        <div class="chart-tooltip-item">
            <span>Group ID:</span>
            <span class="chart-tooltip-value">${item.groupId}</span>
        </div>
        <div class="chart-tooltip-item">
            <span>Partition:</span>
            <span class="chart-tooltip-value">${item.partition}</span>
        </div>
    `;
    
    // Position tooltip closer to mouse
    chartTooltip.style.display = 'block';
    chartTooltip.style.left = (x + 10) + 'px';
    chartTooltip.style.top = (y + 10) + 'px';
}

// Update the Kafka lag chart with new data
function updateKafkaLagChart(lagInfo) {
    if (!kafkaLagChart) {
        // Initialize chart if not already done
        initializeKafkaLagChart();
    }
    
    // Aggregate lag info by topic
    const topicLags = {};
    lagInfo.forEach(info => {
        if (!topicLags[info.topic]) {
            topicLags[info.topic] = {
                topic: info.topic,
                groupId: info.groupId,
                partition: info.partition,
                lag: 0
            };
        }
        topicLags[info.topic].lag += info.lag;
    });
    
    kafkaLagChart.data = Object.values(topicLags);
    
    // Redraw chart
    drawKafkaLagChart();
}

// Log Clusters Chart implementation
function initializeLogClustersChart() {
    const canvas = document.getElementById('log-clusters-chart');
    if (!canvas) return;
    
    const ctx = canvas.getContext('2d');
    logClustersChart = {
        canvas: canvas,
        ctx: ctx,
        data: [],
        hideLowSeverity: false,
        hoveredBar: null
    };
    
    // Set canvas size
    resizeCanvas(canvas);
    
    // Add event listeners
    canvas.addEventListener('mousemove', handleLogClustersChartMouseMove);
    canvas.addEventListener('mouseout', handleLogClustersChartMouseOut);
    
    // Draw initial empty chart
    drawLogClustersChart();
}

function drawLogClustersChart() {
    if (!logClustersChart) return;
    
    const { ctx, canvas, data, hideLowSeverity } = logClustersChart;
    
    // Clear canvas
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    
    // Set background
    ctx.fillStyle = '#252526';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    
    // Filter data if needed
    let filteredData = data;
    if (hideLowSeverity) {
        filteredData = data.filter(item =>
            item.level !== 'DEBUG' && item.level !== 'INFO' && item.level !== 'UNKNOWN'
        );
    }
    
    // Sort by count descending
    const sortedData = [...filteredData].sort((a, b) => b.count - a.count);
    
    // If no data, show message
    if (sortedData.length === 0) {
        ctx.fillStyle = '#d4d4d4';
        ctx.font = '14px JetBrains Mono';
        ctx.textAlign = 'center';
        ctx.fillText('No cluster data available', canvas.width / 2, canvas.height / 2);
        return;
    }
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Find max count for scaling
    const maxCount = Math.max(...sortedData.map(item => item.count), 1);
    
    // Draw bars
    const barWidth = Math.max(2, chartWidth / Math.max(1, sortedData.length) - 4);
    const barSpacing = 4;
    
    ctx.font = '10px JetBrains Mono';
    ctx.textAlign = 'center';
    
    for (let i = 0; i < sortedData.length; i++) {
        const item = sortedData[i];
        const x = margin.left + (i * (barWidth + barSpacing));
        const barHeight = Math.max(1, (item.count / maxCount) * chartHeight);
        const y = margin.top + chartHeight - barHeight;
        
        // Get color based on level
        const color = getLevelColor(item.level);
        
        // Draw bar
        ctx.fillStyle = color;
        ctx.fillRect(x, y, barWidth, barHeight);
        
        // Draw cluster pattern (truncated if too long)
        const patternLabel = item.messagePattern.length > 15 ?
            item.messagePattern.substring(0, 12) + '...' : item.messagePattern;
        ctx.fillStyle = '#d4d4d4';
        ctx.fillText(patternLabel, x + barWidth / 2, margin.top + chartHeight + 15);
        
        // Draw count
        ctx.fillText(item.count.toString(), x + barWidth / 2, y - 5);
    }
    
    // Draw axes
    ctx.strokeStyle = '#d4d4d4';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(margin.left, margin.top);
    ctx.lineTo(margin.left, margin.top + chartHeight);
    ctx.lineTo(canvas.width - margin.right, margin.top + chartHeight);
    ctx.stroke();
    
    // Draw title
    ctx.fillStyle = '#d4d4d4';
    ctx.font = '12px JetBrains Mono';
    ctx.textAlign = 'left';
    ctx.fillText('Log Cluster Distribution by Level', margin.left, margin.top - 10);
}

function getLevelColor(level) {
    const levelColors = {
        'INFO': '#4CAF50',
        'WARN': '#FF9800',
        'ERROR': '#F44336',
        'DEBUG': '#607D8B',
        'KAFKA': '#00BCD4',
        'UNKNOWN': '#9E9E9E'
    };
    return levelColors[level] || '#d4d4d4';
}

function handleLogClustersChartMouseMove(e) {
    if (!logClustersChart || logClustersChart.data.length === 0) return;
    
    const { canvas, data } = logClustersChart;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    
    // Chart dimensions
    const margin = { top: 30, right: 20, bottom: 40, left: 50 };
    const chartWidth = canvas.width - margin.left - margin.right;
    const chartHeight = canvas.height - margin.top - margin.bottom;
    
    // Check if mouse is over chart area
    if (x < margin.left || x > canvas.width - margin.right ||
        y < margin.top || y > margin.top + chartHeight) {
        logClustersChart.hoveredBar = null;
        hideChartTooltip();
        return;
    }
    
    // Find which bar is hovered
    let filteredData = data;
    if (logClustersChart.hideLowSeverity) {
        filteredData = data.filter(item =>
            item.level !== 'DEBUG' && item.level !== 'INFO' && item.level !== 'UNKNOWN'
        );
    }
    
    // Sort by count descending
    const sortedData = [...filteredData].sort((a, b) => b.count - a.count);
    
    const barWidth = Math.max(2, chartWidth / Math.max(1, sortedData.length) - 4);
    const barSpacing = 4;
    
    for (let i = 0; i < sortedData.length; i++) {
        const item = sortedData[i];
        const barX = margin.left + (i * (barWidth + barSpacing));
        
        // Check if mouse is over this bar
        if (x >= barX && x <= barX + barWidth) {
            logClustersChart.hoveredBar = i;
            showLogClustersChartTooltip(item, x, y);
            return;
        }
    }
    
    logClustersChart.hoveredBar = null;
    hideChartTooltip();
}

function handleLogClustersChartMouseOut() {
    if (logClustersChart) {
        logClustersChart.hoveredBar = null;
        hideChartTooltip();
    }
}

function showLogClustersChartTooltip(item, x, y) {
    if (!chartTooltip) return;
    
    chartTooltip.innerHTML = `
        <div class="chart-tooltip-header">${item.level}</div>
        <div class="chart-tooltip-item">
            <span>Count:</span>
            <span class="chart-tooltip-value">${item.count}</span>
        </div>
        <div class="chart-tooltip-item">
            <span>Pattern:</span>
            <span class="chart-tooltip-value">${item.messagePattern}</span>
        </div>
    `;
    
    // Position tooltip closer to mouse
    chartTooltip.style.display = 'block';
    chartTooltip.style.left = (x + 10) + 'px';
    chartTooltip.style.top = (y + 10) + 'px';
}

// Update the log clusters chart with new data
function updateLogClustersChart(clusters) {
    if (!logClustersChart) {
        // Initialize chart if not already done
        initializeLogClustersChart();
    }
    
    logClustersChart.data = clusters;
    
    // Redraw chart
    drawLogClustersChart();
}

// Add event listeners for chart initialization when modals are opened
document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('kafka-lag-btn').addEventListener('click', () => {
        setTimeout(() => {
            if (!kafkaLagChart) {
                initializeKafkaLagChart();
            }
        }, 100);
    });
    
    document.getElementById('log-clusters-btn').addEventListener('click', () => {
        setTimeout(() => {
            if (!logClustersChart) {
                initializeLogClustersChart();
            }
        }, 100);
    });
});