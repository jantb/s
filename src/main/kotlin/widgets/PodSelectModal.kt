package widgets

import SlidePanel
import State
import app.*
import kotlinx.coroutines.channels.trySendBlocking
import java.awt.event.KeyEvent

/**
 * Modern modal-based pod selector
 */
class PodSelectModal(panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : 
    BaseSelector<PodSelectModal.PodItem>(panel, x, y, width, height) {
    
    override fun getTitle(): String = "Pod Selection"
    
    override fun getShortcutKey(): String = "Cmd+P"
    
    override fun shouldOpenModal(e: KeyEvent): Boolean {
        return ((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_P
    }
    
    override fun getItemDisplayText(item: PodItem): String {
        return "${item.name} ${item.version} (${item.creationDate})"
    }
    
    override fun refreshItems() {
        // Store current selection state before refreshing
        val previouslySelected = items.filter { it.selected }.map { it.name }.toSet()
        
        // Get fresh list of pods
        val listPods = ListPods()
        Channels.podsChannel.put(listPods)
        
        try {
            val pods = listPods.result.get()
            items.clear()
            items.addAll(pods.map { pod ->
                PodItem(
                    name = pod.name,
                    version = pod.version,
                    creationDate = pod.creationTimestamp,
                    selected = previouslySelected.contains(pod.name)
                )
            })
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
    
    override fun onItemSelected(item: PodItem) {
        // Don't start listening immediately, wait for modal close
    }
    
    override fun onItemDeselected(item: PodItem) {
        // Stop listening to this specific pod immediately and clear its cache
        Channels.podsChannel.put(UnListenToPod(podName = item.name))
        // Clear the cache entries for this pod
        Channels.popChannel.trySendBlocking(ClearNamedIndex(name = item.name))
    }
    
    override fun onModalClosed() {
        // Apply final selection state when modal is closed
        val selectedPods = items.filter { it.selected }
        val unselectedPods = items.filter { !it.selected }
        
        // Stop listening to unselected pods and clear their cache
        unselectedPods.forEach { pod ->
            Channels.podsChannel.put(UnListenToPod(podName = pod.name))
            Channels.popChannel.trySendBlocking(ClearNamedIndex(name = pod.name))
        }
        
        // Start listening to selected pods
        selectedPods.forEach { pod ->
            Channels.podsChannel.put(ListenToPod(podName = pod.name))
        }
        
        // Trigger a search query to start displaying streaming data
        if (selectedPods.isNotEmpty()) {
            Channels.searchChannel.trySendBlocking(
                QueryChanged(
                    query = "",
                    length = State.length.get(),
                    offset = State.offset.get()
                )
            )
        }
    }
    
    /**
     * Data class representing a selectable pod item
     */
    data class PodItem(
        val name: String,
        val version: String = "",
        val creationDate: String,
        override var selected: Boolean
    ) : SelectableItem
}