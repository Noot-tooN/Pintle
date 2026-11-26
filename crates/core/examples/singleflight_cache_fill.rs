//! Native example for one immutable cache key. Joins model requests that reach
//! its authority before the origin read finishes.
use pintle_core::singleflight::{Completion, Group, Observation, Outcome};
use std::collections::HashMap;

fn main() {
    let key = "catalog:v7:blue-shirt";
    let mut cache = HashMap::new();
    let mut group = Group::default();
    let mut tickets = Vec::new();
    for _ in 0..10 {
        assert!(!cache.contains_key(key));
        tickets.push(group.join(0).unwrap());
    }

    let mut origin_reads = 0;
    for &ticket in &tickets {
        if ticket.is_leader() {
            let value = cache.entry(key).or_insert_with(|| {
                origin_reads += 1;
                b"blue shirt, size M".to_vec()
            });
            group.complete(1, ticket, Completion::Value(value)).unwrap();
        }
    }
    for ticket in tickets {
        assert_eq!(
            group.observe(2, ticket).unwrap(),
            Observation::Finished(Outcome::Value(cache[key].clone()))
        );
    }
    assert_eq!(origin_reads, 1);
    println!("10 overlapping misses for {key}, 1 origin read, 10 matching results");
}
