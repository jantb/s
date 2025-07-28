// WebSocket connection management
let socket;
let reconnectInterval = 1000;
let maxReconnectInterval = 30000;
let reconnectTimer = null;
let isConnected = false;
let pingInterval = null;

function connectWebSocket() {
    if (socket) {
        socket.onclose = null;
        socket.onerror = null;
        socket.close();
    }

    clearInterval(pingInterval);
    document.getElementById('stats-counter').textContent = `Connecting...`;

    socket = new WebSocket(`ws://${location.host}/logs`);

    socket.onopen = () => {
        isConnected = true;
        reconnectInterval = 1000;

        if (!logViewer) {
            initVirtualScroll();
        }

        if (searchQuery) {
            fetchLogs(currentOffset);
        }

        if (selectedPods.size > 0) {
            safeSend({ action: 'listPods' });
            selectedPods.forEach(pod => {
                safeSend({
                    action: 'listenPod',
                    podName: pod
                });
            });
        }

        pingInterval = setInterval(() => {
            if (socket.readyState === WebSocket.OPEN) {
                safeSend({ action: 'ping' });
            } else {
                clearInterval(pingInterval);
            }
        }, 30000); // Reduced ping frequency to match server settings

        document.getElementById('stats-counter').textContent = `Connected - waiting for data...`;
        
        // Request initial data load without search query
        safeSend({
            action: 'search',
            query: '',
            offset: 0,
            length: maxVisibleLines,
            levels: Array.from(activeLevels)
        });
    };

    socket.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);

            if (data.type === 'logs') {
                handleLogData(data.logs);
            } else if (data.type === 'pods') {
                handlePodData(data.podMaps);
            } else if (data.type === 'stats') {
                updateStats(data);
            } else if (data.type === 'logClusters') {
                handleLogClusters(data.logClusters);
            } else if (data.type === 'topics') {
                handleTopics(data.topics);
            } else if (data.type === 'lagInfo') {
                handleLagInfo(data.lagInfo);
            } else if (data.type === 'welcome') {
                // Welcome message received
            }
        } catch (e) {
            // Error parsing message
        }
    };

    socket.onclose = (event) => {
        console.log(`WebSocket closed: code=${event.code}, reason=${event.reason}`);
        document.getElementById('stats-counter').textContent = `Disconnected - reconnecting...`;
        isConnected = false;
        clearInterval(pingInterval);
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(() => {
            reconnectInterval = Math.min(reconnectInterval * 1.5, maxReconnectInterval);
            connectWebSocket();
        }, reconnectInterval);
    };

    socket.onerror = (error) => {
        console.error('WebSocket error:', error);
        socket.close();
    };
}

function updateStats(stats) {
    document.getElementById('stats-counter').textContent = `Indexed Lines: ${stats.indexedLines}`;
}

function safeSend(data) {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
        return false;
    }

    try {
        socket.send(JSON.stringify(data));
        return true;
    } catch (e) {
        return false;
    }
}

function handleLogData(logs) {
    const searchStatus = document.getElementById('search-status');
    isLoading = false;

    if (logs.length === 0) {
        searchStatus.textContent = `No logs at offset ${lastSentOffset}`;
        console.log('No logs received');
        renderVisibleLogs();
        return;
    }

    // Update current logs directly without caching
    currentLogs = logs;

    searchStatus.textContent = `Showing ${logs.length} logs from offset ${lastSentOffset}`;
    console.log(`Received ${logs.length} logs at offset ${lastSentOffset}`);

    renderVisibleLogs();
}

function handlePodData(pods) {
    allPods = pods;
    renderPodList();
}

function handleTopics(topics) {
    allTopics = topics;
    renderTopicsList();
}

function handleLogClusters(clusters) {
    logClusters = clusters;
    renderLogClustersList();
}

function handleLagInfo(lagInfo) {
    console.log('Lag info received:', lagInfo);
    renderLagInfo(lagInfo);
}

// Initialize WebSocket connection
connectWebSocket();