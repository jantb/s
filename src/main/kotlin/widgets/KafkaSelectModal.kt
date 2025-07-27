package widgets

import SlidePanel
import State
import app.*
import kotlinx.coroutines.channels.trySendBlocking
import java.awt.event.KeyEvent

/**
 * Modern modal-based Kafka topic selector
 */
class KafkaSelectModal(panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : 
    BaseSelector<KafkaSelectModal.TopicItem>(panel, x, y, width, height) {
    
    override fun getTitle(): String = "Kafka Topic Selection"
    
    override fun getShortcutKey(): String = "Cmd+K"
    
    override fun shouldOpenModal(e: KeyEvent): Boolean {
        return ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_K
    }
    
    override fun getItemDisplayText(item: TopicItem): String {
        return item.name
    }
    
    override fun refreshItems() {
        // Store current selection state before refreshing
        val previouslySelected = items.filter { it.selected }.map { it.name }.toSet()
        
        // Get fresh list of topics
        val listTopics = ListTopics()
        Channels.kafkaChannel.put(listTopics)
        
        try {
            val topics = listTopics.result.get()
            items.clear()
            items.addAll(topics.map { topicName ->
                TopicItem(
                    name = topicName,
                    selected = previouslySelected.contains(topicName)
                )
            })
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
    
    override fun onItemSelected(item: TopicItem) {
        // Individual topic selection is handled in onModalClosed
        // to avoid multiple listener updates
    }
    
    override fun onItemDeselected(item: TopicItem) {
        // Stop listening to this specific topic immediately and clear its cache
        Channels.kafkaChannel.put(UnListenToTopics)
        // Clear the cache entries for all partitions of this topic
        // Kafka uses indexIdentifier format: "topicName#partitionNumber"
        // We need to clear all partitions, so we'll use a pattern-based approach
        clearTopicFromIndex(item.name)
    }
    
    override fun onModalClosed() {
        // Apply final selection state when modal is closed
        val selectedTopics = items.filter { it.selected }
        val unselectedTopics = items.filter { !it.selected }
        
        // Stop listening to all topics first
        Channels.kafkaChannel.put(UnListenToTopics)
        
        // Clear the cache entries for all partitions of unselected topics
        unselectedTopics.forEach { topic ->
            clearTopicFromIndex(topic.name)
        }
        
        // Start listening to selected topics
        if (selectedTopics.isNotEmpty()) {
            Channels.kafkaChannel.put(ListenToTopic(name = selectedTopics.map { it.name }))
        }
        
        // Trigger a search query to start displaying streaming data
        Channels.searchChannel.trySendBlocking(
            QueryChanged(
                query = "",
                length = State.length.get(),
                offset = State.offset.get()
            )
        )
    }
    
    /**
     * Clear all partition indexes for a given topic
     * Kafka uses indexIdentifier format: "topicName#partitionNumber"
     */
    private fun clearTopicFromIndex(topicName: String) {
        // Use the new ClearTopicIndexes command to efficiently clear all partitions
        Channels.popChannel.trySendBlocking(ClearTopicIndexes(topicName = topicName))
    }
    
    override fun keyPressed(e: KeyEvent) {
        // Handle Kafka-specific shortcuts
        if (isModalOpen) {
            when {
                e.keyCode == KeyEvent.VK_UP && ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) -> {
                    State.kafkaDays.incrementAndGet()
                    panel.repaint()
                    return
                }
                e.keyCode == KeyEvent.VK_DOWN && ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) -> {
                    if (State.kafkaDays.get() > 0) {
                        State.kafkaDays.decrementAndGet()
                    }
                    panel.repaint()
                    return
                }
            }
        }
        
        // Call parent implementation for standard behavior
        super.keyPressed(e)
    }
    
    /**
     * Data class representing a selectable Kafka topic item
     */
    data class TopicItem(
        val name: String,
        override var selected: Boolean
    ) : SelectableItem
}