package gostage

const defaultCacheSize = 1000

// cacheWorkflow stores a workflow in the engine's cache for timer-based recovery.
// Uses O(1) LRU eviction via a doubly-linked list + map node index.
// Front of the list = most recently used; back = least recently used.
func (e *Engine) cacheWorkflow(wf *Workflow) {
	e.workflowCacheMu.Lock()
	defer e.workflowCacheMu.Unlock()

	if node, exists := e.workflowCacheNode[wf.ID]; exists {
		// Cache hit: promote to front (O(1)).
		e.workflowCacheList.MoveToFront(node)
	} else {
		// New entry: evict LRU if at capacity (skip pinned entries).
		if e.cacheSize > 0 && len(e.workflowCache) >= e.cacheSize {
			evicted := false
			for el := e.workflowCacheList.Back(); el != nil; el = el.Prev() {
				id := el.Value.(string)
				if e.pinnedWorkflows[id] == 0 {
					e.workflowCacheList.Remove(el)
					delete(e.workflowCacheNode, id)
					delete(e.workflowCache, id)
					evicted = true
					break
				}
			}
			if !evicted {
				// All entries are pinned — allow cache to grow beyond limit.
				// This is safer than losing a sleeping workflow.
				e.logger.Warn("workflow cache exceeds limit: all %d entries are pinned by sleeping runs", len(e.workflowCache))
			}
		}
		node := e.workflowCacheList.PushFront(wf.ID)
		e.workflowCacheNode[wf.ID] = node
	}
	e.workflowCache[wf.ID] = wf
}

// lookupCachedWorkflow retrieves a workflow from the cache.
func (e *Engine) lookupCachedWorkflow(id string) *Workflow {
	e.workflowCacheMu.RLock()
	defer e.workflowCacheMu.RUnlock()
	return e.workflowCache[id]
}
