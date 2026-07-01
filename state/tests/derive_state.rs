use allex_state::{HasUpdate, LocalState, State, state_type};
use tokio_stream::StreamExt;

// ===========================================================================
// Part 1: #[derive(State)] — backward-compatible, no field hiding
// ===========================================================================

#[derive(State, Clone, Debug, PartialEq)]
pub struct Leaf {
    #[state]
    value: i32,
    #[state]
    label: String,
    ignored: bool,
}

#[test]
fn atomic_set_suppression() {
    let mut leaf = Leaf {
        value: 5,
        label: "hi".into(),
        ignored: false,
    };
    assert!(leaf.set_value(5).is_none(), "same value must be suppressed");
    assert!(leaf.set_value(6).is_some(), "new value must produce update");
    assert_eq!(leaf.value, 6);
}

#[test]
fn atomic_apply_roundtrip() {
    let mut leaf = Leaf {
        value: 0,
        label: "x".into(),
        ignored: false,
    };
    let update = leaf.set_value(42).unwrap();
    let mut target = Leaf {
        value: 0,
        label: "x".into(),
        ignored: false,
    };
    target.apply_update(update);
    assert_eq!(target.value, 42);
}

// ---------------------------------------------------------------------------

#[derive(State, Clone, Debug, PartialEq)]
struct Parent {
    #[state(nested)]
    child: Leaf,
    #[state]
    name: String,
}

#[test]
fn nested_mutation_propagates() {
    let mut parent = Parent {
        child: Leaf {
            value: 0,
            label: "a".into(),
            ignored: false,
        },
        name: "root".into(),
    };
    let update = parent.mutate_child(|c| c.set_value(7));
    assert!(update.is_some(), "nested change must produce update");
    assert_eq!(parent.child.value, 7);
}

#[test]
fn nested_no_op_suppressed() {
    let mut parent = Parent {
        child: Leaf {
            value: 3,
            label: "b".into(),
            ignored: false,
        },
        name: "root".into(),
    };
    let update = parent.mutate_child(|c| c.set_value(3));
    assert!(update.is_none(), "nested no-op must be suppressed");
}

// ---------------------------------------------------------------------------

#[derive(State, Clone, Debug, PartialEq)]
struct Container {
    #[state(vec)]
    items: Vec<Leaf>,
    #[state]
    count: usize,
}

#[test]
fn vec_push() {
    let mut c = Container {
        items: vec![],
        count: 0,
    };
    let upd = c.push_items(Leaf {
        value: 1,
        label: "one".into(),
        ignored: false,
    });
    assert_eq!(c.items.len(), 1);
    let mut target = Container {
        items: vec![],
        count: 0,
    };
    target.apply_update(upd);
    assert_eq!(target.items.len(), 1);
}

#[test]
fn vec_remove() {
    let mut c = Container {
        items: vec![
            Leaf {
                value: 1,
                label: "a".into(),
                ignored: false,
            },
            Leaf {
                value: 2,
                label: "b".into(),
                ignored: false,
            },
        ],
        count: 2,
    };
    let upd = c.remove_items(0);
    assert!(upd.is_some());
    assert_eq!(c.items.len(), 1);
    assert_eq!(c.items[0].value, 2);
}

#[test]
fn vec_remove_oob_returns_none() {
    let mut c = Container {
        items: vec![],
        count: 0,
    };
    assert!(c.remove_items(99).is_none());
}

#[test]
fn vec_mutate_item() {
    let mut c = Container {
        items: vec![Leaf {
            value: 0,
            label: "x".into(),
            ignored: false,
        }],
        count: 1,
    };
    let upd = c.mutate_items(0, |item| item.set_value(55));
    assert!(upd.is_some());
    assert_eq!(c.items[0].value, 55);
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_state_broadcasts_on_change() {
    let state = LocalState::new(Leaf {
        value: 0,
        label: "z".into(),
        ignored: false,
    });
    let mut stream = Box::pin(state.subscribe());
    let _ = stream.next().await;

    let upd = {
        let mut l = state.current();
        l.set_value(10)
    };
    state.apply(upd);

    let next = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
        .await
        .expect("timeout")
        .expect("stream closed");
    assert_eq!(next.value, 10);
}

#[tokio::test]
async fn local_state_no_broadcast_on_suppression() {
    let state = LocalState::new(Leaf {
        value: 5,
        label: "z".into(),
        ignored: false,
    });
    let upd = {
        let mut l = state.current();
        l.set_value(5)
    };
    state.apply(upd);
    assert!(!state.has_subscribers());
}

// ===========================================================================
// Part 2: #[state_type] — field hiding, new(), getters
// ===========================================================================

#[state_type]
#[derive(Clone, Debug, PartialEq)]
pub struct HLeaf {
    #[state]
    value: i32,
    #[state]
    label: String,
    ignored: bool, // plain — required in new()
}

// Nested type used by HParent — must implement Default
#[state_type]
#[derive(Clone, Debug, PartialEq)]
struct HNested {
    #[state]
    x: i32,
    tag: String,
}

impl Default for HNested {
    fn default() -> Self {
        HNested::new(0, String::new())
    }
}

#[state_type]
#[derive(Clone, Debug, PartialEq)]
struct HParent {
    id: u32, // plain — required in new()
    #[state]
    name: String, // atomic — required in new()
    #[state(nested)]
    nested: HNested, // nested — NOT a param, uses Default
    #[state(vec)]
    items: Vec<HLeaf>, // vec — NOT a param, starts empty
}

// ---------------------------------------------------------------------------
// new() constructor
// ---------------------------------------------------------------------------

#[test]
fn state_type_new_constructor() {
    let leaf = HLeaf::new(7, "hello".into(), false);
    // read via getters
    assert_eq!(*leaf.value(), 7);
    assert_eq!(leaf.label(), "hello");
    assert!(!leaf.ignored);
}

#[test]
fn state_type_new_vec_starts_empty() {
    let parent = HParent::new(1, "alice".into());
    assert_eq!(parent.items().len(), 0);
    assert_eq!(*parent.name(), "alice");
    assert_eq!(parent.id, 1);
}

#[test]
fn state_type_new_nested_defaulted() {
    let parent = HParent::new(1, "alice".into());
    assert_eq!(*parent.nested().x(), 0);
}

// ---------------------------------------------------------------------------
// Getters
// ---------------------------------------------------------------------------

#[test]
fn state_type_getter_returns_ref() {
    let leaf = HLeaf::new(42, "world".into(), false);
    let r: &i32 = leaf.value();
    assert_eq!(*r, 42);
    let s: &str = leaf.label();
    assert_eq!(s, "world");
}

#[test]
fn state_type_vec_getter_returns_slice() {
    let mut parent = HParent::new(1, "alice".into());
    parent.push_items(HLeaf::new(1, "a".into(), false));
    let slice: &[HLeaf] = parent.items();
    assert_eq!(slice.len(), 1);
}

// ---------------------------------------------------------------------------
// Mutations via generated methods
// ---------------------------------------------------------------------------

#[test]
fn state_type_set_suppression() {
    let mut leaf = HLeaf::new(5, "hi".into(), false);
    assert!(leaf.set_value(5).is_none());
    assert!(leaf.set_value(9).is_some());
    assert_eq!(*leaf.value(), 9);
}

#[test]
fn state_type_nested_mutate() {
    let mut parent = HParent::new(1, "p".into());
    let upd = parent.mutate_nested(|n| n.set_x(99));
    assert!(upd.is_some());
    assert_eq!(*parent.nested().x(), 99);
}

#[test]
fn state_type_vec_push_remove_mutate() {
    let mut parent = HParent::new(1, "p".into());
    parent.push_items(HLeaf::new(10, "a".into(), false));
    parent.push_items(HLeaf::new(20, "b".into(), false));
    assert_eq!(parent.items().len(), 2);

    let upd = parent.mutate_items(0, |item| item.set_value(11));
    assert!(upd.is_some());
    assert_eq!(*parent.items()[0].value(), 11);

    let upd = parent.remove_items(0);
    assert!(upd.is_some());
    assert_eq!(parent.items().len(), 1);
    assert_eq!(*parent.items()[0].value(), 20);
}

// ---------------------------------------------------------------------------
// apply_update works through HasUpdate
// ---------------------------------------------------------------------------

#[test]
fn state_type_apply_update() {
    let mut a = HLeaf::new(1, "x".into(), false);
    let upd = a.set_value(77).unwrap();
    let mut b = HLeaf::new(1, "x".into(), false);
    b.apply_update(upd);
    assert_eq!(*b.value(), 77);
}

// ---------------------------------------------------------------------------
// LocalState + #[state_type]
// ---------------------------------------------------------------------------

#[tokio::test]
async fn state_type_local_state_broadcasts() {
    let state = LocalState::new(HLeaf::new(0, "z".into(), false));
    let mut stream = Box::pin(state.subscribe());
    let _ = stream.next().await;

    let upd = {
        let mut l = state.current();
        l.set_value(42)
    };
    state.apply(upd);

    let next = tokio::time::timeout(std::time::Duration::from_millis(100), stream.next())
        .await
        .expect("timeout")
        .expect("stream closed");
    assert_eq!(*next.value(), 42);
}

#[tokio::test]
async fn state_type_local_state_suppresses_no_change() {
    let state = LocalState::new(HLeaf::new(5, "z".into(), false));
    let upd = {
        let mut l = state.current();
        l.set_value(5)
    };
    state.apply(upd);
    assert!(!state.has_subscribers());
}
