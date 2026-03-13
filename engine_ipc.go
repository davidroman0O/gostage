package gostage

// handlerEntry pairs a stable ID with a message handler function.
type handlerEntry struct {
	id      HandlerID
	handler MessageHandler
}

// OnMessage registers a handler for IPC messages of the given type.
// Handlers fire when a child process sends a message via Send(),
// or when Send() is called in the parent process (local routing).
// Returns a HandlerID that can be passed to OffMessage to deregister.
func (e *Engine) OnMessage(msgType string, handler MessageHandler) HandlerID {
	if handler == nil {
		return 0
	}
	id := HandlerID(e.messageHandlerCounter.Add(1))
	e.messageHandlersMu.Lock()
	defer e.messageHandlersMu.Unlock()
	e.messageHandlers[msgType] = append(e.messageHandlers[msgType], handlerEntry{id: id, handler: handler})
	e.messageHandlerIndex[id] = msgType
	return id
}

// OffMessage removes a previously registered handler by its HandlerID.
// If the ID is not found, it is a no-op.
func (e *Engine) OffMessage(id HandlerID) {
	if id == 0 {
		return
	}
	e.messageHandlersMu.Lock()
	defer e.messageHandlersMu.Unlock()

	msgType, ok := e.messageHandlerIndex[id]
	if !ok {
		return
	}
	delete(e.messageHandlerIndex, id)

	entries := e.messageHandlers[msgType]
	for i, entry := range entries {
		if entry.id == id {
			newEntries := make([]handlerEntry, 0, len(entries)-1)
			newEntries = append(newEntries, entries[:i]...)
			newEntries = append(newEntries, entries[i+1:]...)
			e.messageHandlers[msgType] = newEntries
			return
		}
	}
}

// dispatchMessage routes an IPC message to all registered handlers.
func (e *Engine) dispatchMessage(msgType string, payload map[string]any) {
	e.messageHandlersMu.RLock()
	entries := e.messageHandlers[msgType]
	// Also fire wildcard handlers (registered with "*")
	wildcardEntries := e.messageHandlers["*"]
	e.messageHandlersMu.RUnlock()

	for _, entry := range entries {
		fn := entry.handler
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.logger.Error("message handler panicked for %q: %v", msgType, r)
				}
			}()
			fn(msgType, payload)
		}()
	}
	for _, entry := range wildcardEntries {
		fn := entry.handler
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.logger.Error("wildcard message handler panicked for %q: %v", msgType, r)
				}
			}()
			fn(msgType, payload)
		}()
	}
}
