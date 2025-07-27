// Virtual scrolling globals
let selectedPods = new Set();
let allPods = [];
let searchQuery = '';
let allLogs = [];
let activeLevels = new Set(['INFO', 'WARN', 'ERROR', 'DEBUG', 'KAFKA']);
let tempSelectedPods = new Set(); // Temporary selection for modal
const lineHeight = 22;
let viewportHeight = 0;
let maxVisibleLines = 0;
let logViewer = null;
let logContainer = null;
let isLoading = false;
let currentOffset = 0;
let lastSentOffset = -1;

// Initialize the virtual scrolling
function initVirtualScroll() {
    logViewer = document.getElementById('log-viewer');
    logContainer = document.getElementById('log-container');

    viewportHeight = logViewer.clientHeight;
    maxVisibleLines = Math.ceil(viewportHeight / lineHeight) - 2;

    logViewer.addEventListener('wheel', handleScroll);
    window.addEventListener('resize', () => {
        viewportHeight = logViewer.clientHeight;
        maxVisibleLines = Math.ceil(viewportHeight / lineHeight) - 2;
        renderVisibleLogs();
        
        // Resize charts if they exist
        if (logLevelChart && logLevelChart.canvas) {
            resizeCanvas(logLevelChart.canvas);
            drawLogLevelChart();
        }
        if (kafkaLagChart && kafkaLagChart.canvas) {
            resizeCanvas(kafkaLagChart.canvas);
            drawKafkaLagChart();
        }
        if (logClustersChart && logClustersChart.canvas) {
            resizeCanvas(logClustersChart.canvas);
            drawLogClustersChart();
        }
    });

    // Pre-create log line elements
    for (let i = 0; i < maxVisibleLines; i++) {
        const logElement = document.createElement('div');
        logElement.className = 'log-line';
        logElement.style.top = `${i * lineHeight}px`;
        logElement.innerHTML = `
            <span class="timestamp"></span>
            <span class="log-level"></span>
            <span class="log-message"></span>
        `;
        logContainer.appendChild(logElement);
    }

    // Add event listeners for level filters
    document.querySelectorAll('.level-filter').forEach(filter => {
        filter.addEventListener('click', () => {
            const level = filter.dataset.level;
            if (activeLevels.has(level)) {
                activeLevels.delete(level);
                filter.classList.remove('active');
                filter.classList.add('inactive');
            } else {
                activeLevels.add(level);
                filter.classList.remove('inactive');
                filter.classList.add('active');
            }
            // Re-render logs with new filter
            renderVisibleLogs();
            // If we have a search query, re-fetch logs to ensure we have enough matching the filter
            if (searchQuery || activeLevels.size < 5) {
                fetchLogsWithOffset(currentOffset);
            }
        });
    });
    
}

// Debounce function to limit scroll event frequency
function debounce(func, wait) {
    let timeout;
    return function(...args) {
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(this, args), wait);
    };
}

// Handle scroll events
function handleScroll(event) {
    event.preventDefault();
    
    // Prevent scrolling if already loading
    if (isLoading) {
        return;
    }
    
    const delta = Math.sign(event.deltaY);
    const newOffset = Math.max(0, currentOffset + delta * 3);

    if (newOffset !== currentOffset) {
        currentOffset = newOffset;
        fetchLogsWithOffset(currentOffset);
    }
}

// Render logs within the viewport
function renderVisibleLogs() {
    const logLines = logContainer.children;

    // Render visible logs directly from the current batch
    for (let i = 0; i < maxVisibleLines; i++) {
        const logElement = logLines[i];
        if (!logElement) continue;

        const logIndex = currentOffset + i;
        const log = allLogs[logIndex];
        
        if (!log || !activeLevels.has(log.level)) {
            logElement.style.display = 'none';
            continue;
        }

        logElement.style.display = 'flex';
        logElement.className = `log-line level-${log.level}`;
        logElement.style.top = `${i * lineHeight}px`;

        const date = new Date(log.timestamp);
        const timeStr = date.toISOString().replace('T', ' ').substr(0, 19);

        const escapeHtml = (str) => {
            return str
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        };

        const timestampSpan = logElement.querySelector('.timestamp');
        const levelSpan = logElement.querySelector('.log-level');
        const messageSpan = logElement.querySelector('.log-message');

        timestampSpan.textContent = `[${timeStr}]`;
        levelSpan.textContent = `[${log.level}]`;
        messageSpan.textContent = escapeHtml(log.message);
    }
}

// Fetch logs for the given offset
function fetchLogs(offset) {
    if (isLoading) {
        return;
    }

    if (offset === lastSentOffset) {
        renderVisibleLogs();
        return;
    }

    isLoading = true;
    lastSentOffset = offset;
    document.getElementById('search-status').textContent = 'Loading...';

    if (!safeSend({
        action: 'search',
        query: searchQuery,
        offset: offset,
        length: maxVisibleLines,
        levels: Array.from(activeLevels)
    })) {
        console.log('Failed to send search query, connection issue');
        document.getElementById('search-status').textContent = 'Connection lost, reconnecting...';
        isLoading = false;
    }
}

// Fetch logs with offset while maintaining search context
function fetchLogsWithOffset(offset) {
    if (isLoading) {
        return;
    }

    // Don't send duplicate requests
    if (offset === lastSentOffset) {
        renderVisibleLogs();
        return;
    }

    isLoading = true;
    lastSentOffset = offset;
    
    // Update status to show we're loading at this offset
    const statusText = searchQuery ?
        `Searching "${searchQuery}" at offset ${offset}...` :
        `Loading logs at offset ${offset}...`;
    document.getElementById('search-status').textContent = statusText;

    // Send search request with current query and new offset
    if (!safeSend({
        action: 'search',
        query: searchQuery,
        offset: offset,
        length: maxVisibleLines,
        levels: Array.from(activeLevels)
    })) {
        console.log('Failed to send search query, connection issue');
        document.getElementById('search-status').textContent = 'Connection lost, reconnecting...';
        isLoading = false;
    }
}

const sendSearchQuery = debounce((query, resetOffset = true) => {
    console.log(`New search query: ${query}, resetOffset: ${resetOffset}`);
    
    if (resetOffset) {
        // Only reset when it's a new search, not when scrolling
        document.getElementById('search-status').textContent = 'Searching...';
        allLogs = []; // Clear all existing logs
        currentOffset = 0;
        lastSentOffset = -1;
        logContainer.innerHTML = ''; // Clear UI
        logViewer.scrollTop = 0; // Reset scroll position
        
        // Re-initialize log lines
        for (let i = 0; i < maxVisibleLines; i++) {
            const logElement = document.createElement('div');
            logElement.className = 'log-line';
            logElement.style.top = `${i * lineHeight}px`;
            logElement.innerHTML = `
                <span class="timestamp"></span>
                <span class="log-level"></span>
                <span class="log-message"></span>
            `;
            logContainer.appendChild(logElement);
        }
        renderVisibleLogs();
    }

    if (!safeSend({
        action: 'search',
        query: query,
        offset: resetOffset ? 0 : currentOffset,
        length: maxVisibleLines,
        levels: Array.from(activeLevels)
    })) {
        document.getElementById('search-status').textContent = 'Connection lost, reconnecting...';
        if (!resetOffset) {
            isLoading = false; // Reset loading state if this was a scroll request
        }
    }
}, 300);

// Event listeners
document.getElementById('search-input').addEventListener('input', (e) => {
    searchQuery = e.target.value;
    sendSearchQuery(searchQuery, true); // Always send the query, even if blank, reset offset for new searches
});

// Keyboard navigation
document.addEventListener('keydown', (e) => {
    // Ctrl/Cmd + P to open pod selection
    if ((e.ctrlKey || e.metaKey) && e.key === 'p') {
        e.preventDefault();
        document.getElementById('pod-select-btn').click();
    }
    
    // Escape to close pod selection
    if (e.key === 'Escape') {
        const podSelector = document.getElementById('pod-selector');
        if (podSelector.classList.contains('visible')) {
            podSelector.classList.remove('visible');
        }
    }
    
    // Enter in search input to force search
    if (e.key === 'Enter' && e.target.id === 'search-input') {
        sendSearchQuery(searchQuery, true);
    }
    
});