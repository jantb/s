// Modal and UI component functionality
let allTopics = [];
let logClusters = [];

// Pod selection functionality
function renderPodList() {
    const podList = document.getElementById('pod-list');
    podList.innerHTML = '';

    const filterText = document.getElementById('pod-search').value.toLowerCase();

    const filteredPods = filterText ?
        allPods.filter(pod => pod.name.toLowerCase().includes(filterText)) :
        allPods;

    for (const pod of filteredPods) {
        const podItem = document.createElement('div');
        podItem.className = `pod-item ${tempSelectedPods.has(pod.name) ? 'selected' : ''}`;
        podItem.innerHTML = `
            <div class="pod-checkbox"></div>
            <div class="pod-name">${pod.name}</div>
            <div class="pod-version">${pod.version}</div>
        `;

        podItem.addEventListener('click', () => {
            if (tempSelectedPods.has(pod.name)) {
                tempSelectedPods.delete(pod.name);
            } else {
                tempSelectedPods.add(pod.name);
            }
            renderPodList();
        });

        podList.appendChild(podItem);
    }
}

// Kafka topics functionality
function renderTopicsList() {
    const topicsList = document.getElementById('kafka-topics-list');
    topicsList.innerHTML = '';

    const filterText = document.getElementById('kafka-topics-search').value.toLowerCase();

    const filteredTopics = filterText ?
        allTopics.filter(topic => topic.name.toLowerCase().includes(filterText)) :
        allTopics;

    for (const topic of filteredTopics) {
        const topicItem = document.createElement('div');
        topicItem.className = 'pod-item';
        topicItem.innerHTML = `
            <div class="pod-name">${topic.name}</div>
        `;

        topicItem.addEventListener('click', () => {
            // For now, just log the click
            console.log('Topic clicked:', topic.name);
        });

        topicsList.appendChild(topicItem);
    }
}

// Log clusters functionality
function renderLogClustersList() {
    const clustersList = document.getElementById('log-clusters-list');
    clustersList.innerHTML = '';

    // Sort log clusters by count in descending order (most to least)
    const sortedClusters = [...logClusters].sort((a, b) => b.count - a.count);

    for (const cluster of sortedClusters) {
        const clusterItem = document.createElement('div');
        clusterItem.className = 'pod-item';
        clusterItem.innerHTML = `
            <div class="pod-name">${cluster.messagePattern}</div>
            <div class="pod-version">${cluster.count} occurrences</div>
        `;

        clusterItem.addEventListener('click', () => {
            // For now, just log the click
            console.log('Cluster clicked:', cluster.messagePattern);
        });

        clustersList.appendChild(clusterItem);
    }
}

// Kafka lag info rendering
function renderLagInfo(lagInfo) {
    const lagList = document.getElementById('kafka-lag-list');
    lagList.innerHTML = '';
    
    if (lagInfo.length === 0) {
        const noDataItem = document.createElement('div');
        noDataItem.className = 'pod-item';
        noDataItem.textContent = 'No consumer lag information available.';
        lagList.appendChild(noDataItem);
        return;
    }
    
    // Filter out entries with 0 lag
    const nonZeroLagInfo = lagInfo.filter(info => info.lag > 0);
    
    // If all entries have 0 lag, show a message
    if (nonZeroLagInfo.length === 0) {
        const noLagItem = document.createElement('div');
        noLagItem.className = 'pod-item';
        noLagItem.textContent = 'All topics have zero lag.';
        lagList.appendChild(noLagItem);
        return;
    }
    
    // Group lag info by topic
    const topicLags = {};
    nonZeroLagInfo.forEach(info => {
        if (!topicLags[info.topic]) {
            topicLags[info.topic] = [];
        }
        topicLags[info.topic].push(info);
    });
    
    // Create an array of topics with their total lag for sorting
    const topicsWithTotalLag = Object.entries(topicLags).map(([topic, infos]) => {
        const totalLag = infos.reduce((sum, info) => sum + info.lag, 0);
        return { topic, infos, totalLag };
    });
    
    // Sort topics by total lag in descending order (most lag on top)
    topicsWithTotalLag.sort((a, b) => b.totalLag - a.totalLag);
    
    // Render lag info for each topic
    topicsWithTotalLag.forEach(({ topic, infos, totalLag }) => {
        const topicHeader = document.createElement('div');
        topicHeader.className = 'pod-item';
        topicHeader.innerHTML = `<div class="pod-name"><strong>${topic}</strong></div>`;
        lagList.appendChild(topicHeader);
        
        const totalItem = document.createElement('div');
        totalItem.className = 'pod-item';
        totalItem.innerHTML = `<div class="pod-name">Total Lag</div><div class="pod-version">${totalLag}</div>`;
        lagList.appendChild(totalItem);
        
        // Sort partitions by lag in descending order
        const sortedInfos = [...infos].sort((a, b) => b.lag - a.lag);
        
        // Render individual partition info
        sortedInfos.forEach(info => {
            const partitionItem = document.createElement('div');
            partitionItem.className = 'pod-item';
            partitionItem.innerHTML = `
                <div class="pod-name">Partition ${info.partition}</div>
                <div class="pod-version">
                    ${info.currentOffset} → ${info.endOffset} (Lag: ${info.lag})
                </div>
            `;
            lagList.appendChild(partitionItem);
        });
    });
}

// Add days selection to Kafka topics
function addDaysSelectionToTopicsModal() {
    const modalHeader = document.querySelector('.kafka-topics-selector .pod-modal-header');
    if (!modalHeader) return;
    
    // Check if days selection already exists
    if (document.getElementById('kafka-days-selection')) return;
    
    const daysContainer = document.createElement('div');
    daysContainer.id = 'kafka-days-selection';
    daysContainer.style.display = 'flex';
    daysContainer.style.alignItems = 'center';
    daysContainer.style.marginLeft = '20px';
    
    const label = document.createElement('span');
    label.textContent = 'Days back: ';
    label.style.marginRight = '8px';
    label.style.color = '#d4d4d4';
    
    const select = document.createElement('select');
    select.id = 'kafka-days-select';
    select.style.padding = '4px 8px';
    select.style.backgroundColor = '#3c3c3c';
    select.style.border = '1px solid #555';
    select.style.color = '#d4d4d4';
    select.style.borderRadius = '3px';
    
    // Add options for days (0-30)
    for (let i = 0; i <= 30; i++) {
        const option = document.createElement('option');
        option.value = i;
        option.textContent = i;
        if (i === 1) option.selected = true; // Default to 1 day
        select.appendChild(option);
    }
    
    select.addEventListener('change', (e) => {
        const days = parseInt(e.target.value);
        // Send message to backend to update days
        safeSend({
            action: 'setKafkaDays',
            days: days
        });
    });
    
    daysContainer.appendChild(label);
    daysContainer.appendChild(select);
    modalHeader.appendChild(daysContainer);
}

// Event listeners for modals
document.addEventListener('DOMContentLoaded', () => {
    // Pod selection modal
    document.getElementById('pod-select-btn').addEventListener('click', () => {
        // Copy current selections to temp set when opening modal
        tempSelectedPods = new Set(selectedPods);
        document.getElementById('pod-selector').classList.add('visible');
        safeSend({
            action: 'listPods'
        });
    });

    document.getElementById('close-pod-selector-btn').addEventListener('click', () => {
        document.getElementById('pod-selector').classList.remove('visible');
    });

    document.getElementById('apply-pod-selection-btn').addEventListener('click', () => {
        // Apply temp selections to actual selections
        const podsToUnlisten = [...selectedPods].filter(pod => !tempSelectedPods.has(pod));
        const podsToListen = [...tempSelectedPods].filter(pod => !selectedPods.has(pod));
        
        // Unlisten from pods that were deselected
        podsToUnlisten.forEach(pod => {
            selectedPods.delete(pod);
            safeSend({
                action: 'unlistenPod',
                podName: pod
            });
        });
        
        // Listen to pods that were selected
        podsToListen.forEach(pod => {
            selectedPods.add(pod);
            safeSend({
                action: 'listenPod',
                podName: pod
            });
        });
        
        document.getElementById('pod-selector').classList.remove('visible');
        
        // Trigger a new search to fetch logs from the newly selected pods
        if (podsToListen.length > 0) {
            // Clear current logs and reset offset
            currentLogs = [];
            currentOffset = 0;
            lastSentOffset = -1;
            
            // Trigger search to get logs from new pods
            setTimeout(() => {
                fetchLogs(0);
            }, 100); // Small delay to ensure pod listening is established
        }
    });

    document.getElementById('refresh-pods-btn').addEventListener('click', () => {
        safeSend({
            action: 'listPods'
        });
    });

    document.getElementById('pod-search').addEventListener('input', () => {
        renderPodList();
    });

    // Kafka topics modal
    document.getElementById('kafka-topics-btn').addEventListener('click', () => {
        document.getElementById('kafka-topics-selector').classList.add('visible');
        safeSend({
            action: 'listTopics'
        });
        setTimeout(addDaysSelectionToTopicsModal, 100);
    });

    document.getElementById('close-kafka-topics-btn').addEventListener('click', () => {
        document.getElementById('kafka-topics-selector').classList.remove('visible');
    });

    document.getElementById('refresh-kafka-topics-btn').addEventListener('click', () => {
        safeSend({
            action: 'listTopics'
        });
    });

    document.getElementById('apply-kafka-topics-btn').addEventListener('click', () => {
        // For now, just close the modal
        // In a real implementation, we would apply the topic selection
        document.getElementById('kafka-topics-selector').classList.remove('visible');
    });

    document.getElementById('kafka-topics-search').addEventListener('input', () => {
        renderTopicsList();
    });

    // Log clusters modal
    document.getElementById('log-clusters-btn').addEventListener('click', () => {
        document.getElementById('log-clusters-selector').classList.add('visible');
        // Request log clusters data
        safeSend({
            action: 'refreshLogGroups'
        });
    });

    document.getElementById('close-log-clusters-btn').addEventListener('click', () => {
        document.getElementById('log-clusters-selector').classList.remove('visible');
    });

    // Kafka lag modal
    document.getElementById('kafka-lag-btn').addEventListener('click', () => {
        document.getElementById('kafka-lag-selector').classList.add('visible');
        safeSend({
            action: 'listLag'
        });
    });
    
    document.getElementById('close-kafka-lag-btn').addEventListener('click', () => {
        document.getElementById('kafka-lag-selector').classList.remove('visible');
    });
    
    document.getElementById('refresh-kafka-lag-btn').addEventListener('click', () => {
        safeSend({
            action: 'listLag'
        });
    });
});