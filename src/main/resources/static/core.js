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

// Virtual cache globals
const VIRTUAL_CACHE_SIZE = 11000; // Total lines to keep in virtual cache
const BORDER_THRESHOLD = 1000; // When to request more data (distance from border)
const REQUEST_CHUNK_SIZE = 2000; // How many lines to request at once

let virtualCache = []; // Main virtual cache array
let virtualCacheStartOffset = 0; // Offset where virtual cache starts
let isVirtualCacheInitialized = false;

// Initialize the virtual scrolling
function initVirtualScroll() {
    logViewer = document.getElementById('log-viewer');
    logContainer = document.getElementById('log-container');

    viewportHeight = logViewer.clientHeight;
    maxVisibleLines = Math.ceil(viewportHeight / lineHeight) - 2;
    // Virtual cache is independent of visible lines

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
            // Clear virtual cache when filters change as cached data may not match new filter
            clearVirtualCache();
            // Re-render logs with new filter
            renderVisibleLogs();
            // If we have a search query, re-fetch logs to ensure we have enough matching the filter
            if (searchQuery || activeLevels.size < 5) {
                fetchLogsWithVirtualCache(currentOffset);
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
    
    // Fix scroll direction: negative deltaY (scroll up) should go to older logs (higher offset)
    // positive deltaY (scroll down) should go to newer logs (lower offset)
    const delta = -Math.sign(event.deltaY);
    const newOffset = Math.max(0, currentOffset + delta * 3);

    if (newOffset !== currentOffset) {
        currentOffset = newOffset;
        fetchLogsWithVirtualCache(currentOffset);
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
        const log = getLogFromVirtualCache(logIndex);
        
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

// Virtual cache function to fetch logs with intelligent cache management
function fetchLogsWithVirtualCache(offset) {
    if (isLoading) {
        return;
    }

    // Check if we have this data in virtual cache
    if (isLogInVirtualCache(offset)) {
        console.log(`Virtual cache hit for offset ${offset}`);
        renderVisibleLogs();
        document.getElementById('search-status').textContent = `Showing cached logs from offset ${offset}`;
        
        // Check if we need to expand the cache (approaching borders)
        checkAndExpandVirtualCache(offset);
        return;
    }

    // Don't send duplicate requests
    if (offset === lastSentOffset) {
        renderVisibleLogs();
        return;
    }

    // If virtual cache is not initialized or offset is far from current cache, initialize around this offset
    if (!isVirtualCacheInitialized || !isOffsetNearVirtualCache(offset)) {
        initializeVirtualCacheAt(offset);
        return;
    }

    // Expand cache to include this offset
    expandVirtualCacheToInclude(offset);
}

// Fetch logs with offset while maintaining search context (legacy function for compatibility)
function fetchLogsWithOffset(offset) {
    fetchLogsWithVirtualCache(offset);
}

const sendSearchQuery = debounce((query, resetOffset = true) => {
    console.log(`New search query: ${query}, resetOffset: ${resetOffset}`);
    
    if (resetOffset) {
        // Only reset when it's a new search, not when scrolling
        document.getElementById('search-status').textContent = 'Searching...';
        allLogs = []; // Clear all existing logs
        clearVirtualCache(); // Clear the virtual cache for new searches
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

    const requestOffset = resetOffset ? 0 : currentOffset;
    lastSentOffset = requestOffset;

    if (!safeSend({
        action: 'search',
        query: query,
        offset: requestOffset,
        length: REQUEST_CHUNK_SIZE,
        levels: Array.from(activeLevels)
    })) {
        document.getElementById('search-status').textContent = 'Connection lost, reconnecting...';
        if (!resetOffset) {
            isLoading = false; // Reset loading state if this was a scroll request
        }
    }
}, 300);

// Virtual cache management functions
function clearVirtualCache() {
    virtualCache = [];
    virtualCacheStartOffset = 0;
    isVirtualCacheInitialized = false;
    allLogs = []; // Also clear the old allLogs array
    console.log('Virtual cache cleared');
}

function getLogFromVirtualCache(globalOffset) {
    if (!isVirtualCacheInitialized) {
        return null;
    }
    
    const localIndex = globalOffset - virtualCacheStartOffset;
    if (localIndex >= 0 && localIndex < virtualCache.length) {
        return virtualCache[localIndex];
    }
    
    return null;
}

function isLogInVirtualCache(globalOffset) {
    if (!isVirtualCacheInitialized) {
        return false;
    }
    
    const localIndex = globalOffset - virtualCacheStartOffset;
    return localIndex >= 0 && localIndex < virtualCache.length && virtualCache[localIndex] != null;
}

function isOffsetNearVirtualCache(globalOffset) {
    if (!isVirtualCacheInitialized) {
        return false;
    }
    
    const cacheEndOffset = virtualCacheStartOffset + virtualCache.length;
    return globalOffset >= virtualCacheStartOffset - BORDER_THRESHOLD &&
           globalOffset <= cacheEndOffset + BORDER_THRESHOLD;
}

function checkAndExpandVirtualCache(currentOffset) {
    if (!isVirtualCacheInitialized) {
        return;
    }
    
    const distanceFromStart = currentOffset - virtualCacheStartOffset;
    const distanceFromEnd = (virtualCacheStartOffset + virtualCache.length) - currentOffset;
    
    // If approaching the start border, expand backwards
    if (distanceFromStart < BORDER_THRESHOLD && virtualCacheStartOffset > 0) {
        expandVirtualCacheBackwards();
    }
    
    // If approaching the end border, expand forwards
    if (distanceFromEnd < BORDER_THRESHOLD) {
        expandVirtualCacheForwards();
    }
}

function initializeVirtualCacheAt(offset) {
    console.log(`Initializing virtual cache at offset ${offset}`);
    
    // Calculate optimal start position (center the cache around the requested offset)
    const idealStart = Math.max(0, offset - Math.floor(REQUEST_CHUNK_SIZE / 2));
    virtualCacheStartOffset = idealStart;
    
    isLoading = true;
    lastSentOffset = virtualCacheStartOffset; // Set to the actual request offset
    
    const statusText = searchQuery ?
        `Initializing cache: searching "${searchQuery}" at offset ${virtualCacheStartOffset}...` :
        `Initializing cache at offset ${virtualCacheStartOffset}...`;
    document.getElementById('search-status').textContent = statusText;

    // Request a large chunk to initialize the cache
    if (!safeSend({
        action: 'search',
        query: searchQuery,
        offset: virtualCacheStartOffset,
        length: REQUEST_CHUNK_SIZE,
        levels: Array.from(activeLevels)
    })) {
        console.log('Failed to initialize virtual cache, connection issue');
        document.getElementById('search-status').textContent = 'Connection lost, reconnecting...';
        isLoading = false;
    }
}

function expandVirtualCacheBackwards() {
    if (isLoading || virtualCacheStartOffset <= 0) {
        return;
    }
    
    console.log('Expanding virtual cache backwards');
    
    const newStartOffset = Math.max(0, virtualCacheStartOffset - REQUEST_CHUNK_SIZE);
    const requestSize = virtualCacheStartOffset - newStartOffset;
    
    if (requestSize <= 0) {
        return;
    }
    
    isLoading = true;
    lastSentOffset = newStartOffset;
    
    if (!safeSend({
        action: 'search',
        query: searchQuery,
        offset: newStartOffset,
        length: requestSize,
        levels: Array.from(activeLevels)
    })) {
        console.log('Failed to expand virtual cache backwards');
        isLoading = false;
    }
}

function expandVirtualCacheForwards() {
    if (isLoading) {
        return;
    }
    
    console.log('Expanding virtual cache forwards');
    
    const currentEndOffset = virtualCacheStartOffset + virtualCache.length;
    
    isLoading = true;
    lastSentOffset = currentEndOffset;
    
    if (!safeSend({
        action: 'search',
        query: searchQuery,
        offset: currentEndOffset,
        length: REQUEST_CHUNK_SIZE,
        levels: Array.from(activeLevels)
    })) {
        console.log('Failed to expand virtual cache forwards');
        isLoading = false;
    }
}

function expandVirtualCacheToInclude(offset) {
    if (isLoading) {
        return;
    }
    
    console.log(`Expanding virtual cache to include offset ${offset}`);
    
    const currentEndOffset = virtualCacheStartOffset + virtualCache.length;
    
    if (offset < virtualCacheStartOffset) {
        // Need to expand backwards
        expandVirtualCacheBackwards();
    } else if (offset >= currentEndOffset) {
        // Need to expand forwards
        expandVirtualCacheForwards();
    }
}

function addToVirtualCache(offset, logs, isExpansion = false) {
    console.log(`Adding ${logs.length} logs to virtual cache at offset ${offset}, expansion: ${isExpansion}`);
    
    if (!isVirtualCacheInitialized) {
        // First initialization
        virtualCacheStartOffset = offset;
        virtualCache = [...logs];
        isVirtualCacheInitialized = true;
        console.log(`Virtual cache initialized: start=${virtualCacheStartOffset}, size=${virtualCache.length}`);
    } else if (isExpansion) {
        if (offset < virtualCacheStartOffset) {
            // Expanding backwards
            const newLogs = [...logs];
            virtualCache = newLogs.concat(virtualCache);
            virtualCacheStartOffset = offset;
            
            // Trim if cache is too large
            if (virtualCache.length > VIRTUAL_CACHE_SIZE) {
                const excess = virtualCache.length - VIRTUAL_CACHE_SIZE;
                virtualCache = virtualCache.slice(0, VIRTUAL_CACHE_SIZE);
                console.log(`Trimmed ${excess} logs from end of virtual cache`);
            }
        } else {
            // Expanding forwards
            virtualCache = virtualCache.concat(logs);
            
            // Trim if cache is too large
            if (virtualCache.length > VIRTUAL_CACHE_SIZE) {
                const excess = virtualCache.length - VIRTUAL_CACHE_SIZE;
                virtualCache = virtualCache.slice(excess);
                virtualCacheStartOffset += excess;
                console.log(`Trimmed ${excess} logs from start of virtual cache`);
            }
        }
        
        console.log(`Virtual cache expanded: start=${virtualCacheStartOffset}, size=${virtualCache.length}`);
    } else {
        // Replace existing data
        const localStartIndex = offset - virtualCacheStartOffset;
        for (let i = 0; i < logs.length; i++) {
            const localIndex = localStartIndex + i;
            if (localIndex >= 0 && localIndex < virtualCache.length) {
                virtualCache[localIndex] = logs[i];
            }
        }
    }
    
    // Update allLogs for compatibility
    for (let i = 0; i < logs.length; i++) {
        allLogs[offset + i] = logs[i];
    }
}

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