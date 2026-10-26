//! Bounded, object-local notification hints. Register before reading durable
//! state; a sticky signal covers transitions before the future is first polled.
use std::{
    cell::RefCell,
    task::{Poll, Waker},
};
const MAX_SUBSCRIBERS: usize = 16;
#[derive(Default)]
struct Listener {
    ready: bool,
    waker: Option<Waker>,
}
#[derive(Default)]
pub struct Notifications {
    slots: RefCell<Vec<Option<Listener>>>,
}
pub struct Subscription<'a> {
    hub: &'a Notifications,
    slot: usize,
}
impl Notifications {
    pub fn subscribe(&self) -> Option<Subscription<'_>> {
        let mut slots = self.slots.borrow_mut();
        let slot = if let Some(n) = slots.iter().position(Option::is_none) {
            n
        } else if slots.len() < MAX_SUBSCRIBERS {
            slots.push(None);
            slots.len() - 1
        } else {
            return None;
        };
        slots[slot] = Some(Listener::default());
        Some(Subscription { hub: self, slot })
    }
    pub fn notify(&self) {
        let mut wake = Vec::new();
        for l in self.slots.borrow_mut().iter_mut().flatten() {
            l.ready = true;
            if let Some(w) = l.waker.take() {
                wake.push(w);
            }
        }
        for w in wake {
            w.wake();
        }
    }
}
impl Subscription<'_> {
    pub async fn changed(&self) {
        futures_util::future::poll_fn(|cx| {
            let mut slots = self.hub.slots.borrow_mut();
            let Some(l) = slots[self.slot].as_mut() else {
                return Poll::Ready(());
            };
            if l.ready {
                l.ready = false;
                Poll::Ready(())
            } else {
                l.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}
impl Drop for Subscription<'_> {
    fn drop(&mut self) {
        self.hub.slots.borrow_mut()[self.slot] = None;
    }
}
