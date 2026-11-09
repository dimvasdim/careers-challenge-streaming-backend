export interface SseMessage {
  id?: string;
  event?: string;
  data: string;
}

/** Opens GET /alarms/stream and collects parsed SSE messages as they arrive. */
export async function openStream(
  baseUrl: string,
  headers: Record<string, string> = {},
) {
  const controller = new AbortController();
  const res = await fetch(`${baseUrl}/alarms/stream`, {
    headers,
    signal: controller.signal,
  });
  const messages: SseMessage[] = [];
  let comments = 0;
  const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = '';
  void (async () => {
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) return;
        buffer += value;
        let end: number;
        while ((end = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, end).replace(/^\n+/, ''); // Nest opens with "\n"
          buffer = buffer.slice(end + 2);
          if (block.startsWith(':')) {
            comments++; // keepalive comment, not an event
            continue;
          }
          const msg: SseMessage = { data: '' };
          for (const line of block.split('\n')) {
            const [field, ...rest] = line.split(':');
            const value = rest.join(':').replace(/^ /, '');
            if (field === 'id') msg.id = value;
            else if (field === 'event') msg.event = value;
            else if (field === 'data') msg.data += value;
          }
          messages.push(msg);
        }
      }
    } catch {
      // aborted
    }
  })();
  /** Waits until `count` messages have arrived (or fails after timeoutMs). */
  const waitFor = async (count: number, timeoutMs = 2000) => {
    const deadline = Date.now() + timeoutMs;
    while (messages.length < count) {
      if (Date.now() > deadline)
        throw new Error(`got ${messages.length}/${count} SSE messages`);
      await new Promise((r) => setTimeout(r, 10));
    }
    return messages;
  };
  return {
    status: res.status,
    messages,
    comments: () => comments,
    waitFor,
    close: () => controller.abort(),
  };
}
