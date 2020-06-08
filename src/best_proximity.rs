use std::cmp;

const ONE_ATTRIBUTE: u32 = 1000;
const MAX_INDEX: u32 = ONE_ATTRIBUTE - 1;
const MAX_DISTANCE: u32 = 8;

// Returns the attribute and index parts.
fn extract_position(position: u32) -> (u32, u32) {
    (position / ONE_ATTRIBUTE, position % ONE_ATTRIBUTE)
}

// Returns a position from the two parts of it.
fn construct_position(attr: u32, index: u32) -> u32 {
    attr * ONE_ATTRIBUTE + index
}

// TODO we should use an sdset::Set for `next_positions`.
// Returns the positions to focus that will give the best possible proximity.
fn best_proximity_for(current_position: u32, proximity: u32, next_positions: &[u32]) -> Option<(u32, Vec<u32>)> {
    let (current_attr, _) = extract_position(current_position);

    match proximity {
        // look at i+0
        0 => {
            match next_positions.binary_search(&current_position) {
                Ok(_) => Some((0, vec![current_position])),
                Err(_) => best_proximity_for(current_position, proximity + 1, next_positions),
            }
        },
        // look at i+1
        1 => {
            let position = current_position + 1;
            let (attr, _) = extract_position(position);

            // We must check that we do not overflowed the current attribute. If so,
            // we must check for a bigger proximity that we will be able to find behind.
            if current_attr == attr {
                match next_positions.binary_search(&position) {
                    Ok(_) => Some((1, vec![position])),
                    Err(_) => best_proximity_for(current_position, proximity + 1, next_positions),
                }
            } else {
                best_proximity_for(current_position, proximity + 1, next_positions)
            }
        },
        // look at i-(p-1), i+p
        2..=7 => {
            let mut output = Vec::new();

            // Behind the current_position
            if let Some(position) = current_position.checked_sub(proximity - 1) {
                let (attr, _) = extract_position(position);
                // We must make sure we are not looking at a word at the end of another attribute.
                if current_attr == attr && next_positions.binary_search(&position).is_ok() {
                    output.push(position);
                }
            }

            // In front of the current_position
            let position = current_position + proximity;
            let (attr, _) = extract_position(position);
            // We must make sure we are not looking at a word at the end of another attribute.
            if current_attr == attr && next_positions.binary_search(&position).is_ok() {
                output.push(position);
            }

            if output.is_empty() {
                best_proximity_for(current_position, proximity + 1, next_positions)
            } else {
                Some((proximity, output))
            }
        },
        // look at i+8 and all above and i-(8-1) and all below
        8 => {
            let mut output = Vec::new();

            // Make sure we look at the latest index of the previous attr.
            if let Some(previous_position) = construct_position(current_attr, 0).checked_sub(1) {
                let position = current_position.saturating_sub(7).max(previous_position);
                match dbg!(next_positions.binary_search(&position)) {
                    Ok(i) => output.extend_from_slice(&next_positions[..=i]),
                    Err(i) => if let Some(i) = i.checked_sub(1) {
                        if let Some(positions) = next_positions.get(..=i) {
                            output.extend_from_slice(positions)
                        }
                    },
                }
            }

            // Make sure the position doesn't overflow to the next attribute.
            let position = (current_position + 8).min(construct_position(current_attr + 1, 0));
            match next_positions.binary_search(&position) {
                Ok(i) => output.extend_from_slice(&next_positions[i..]),
                Err(i) => if let Some(positions) = next_positions.get(i..) {
                    output.extend_from_slice(positions);
                },
            }

            if output.is_empty() {
                None
            } else {
                Some((8, output))
            }
        }
        _ => None,
    }
}

pub struct BestProximity {
    positions: Vec<Vec<u32>>,
    current_proximity: Option<(u32, Vec<(u32, usize)>)>, // where we are
}

impl BestProximity {
    pub fn new(positions: Vec<Vec<u32>>) -> BestProximity {
        BestProximity { positions, current_proximity: None }
    }
}

impl Iterator for BestProximity {
    type Item = (u32, Vec<u32>);

    fn next(&mut self) -> Option<Self::Item> {
        let output = Vec::new();
        let best_proximity = 0;

        for (i, positions) in self.positions.iter().enumerate() {
            if let Some(next_positions) = self.positions.get(i + 1) {
                for x in positions {
                    let p = next_positions.binary_search(&x);
                    let y = next_positions.get(p.unwrap_or_else(|p| p));
                    eprintln!("{:?} gives {:?} ({:?})", x, p, y);
                }
            }
        }

        // match &mut self.current_proximity {
        //     Some((_prox, _pos)) => {
        //         // ...
        //     },
        //     None => {
        //         // ...
        //     },
        // }

        Some((best_proximity, output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_attribute() {
        let positions = vec![
            vec![0,    2, 3, 4   ],
            vec![   1,           ],
            vec![         3,    6],
        ];
        let mut iter = BestProximity::new(positions);

        assert_eq!(iter.next(), Some((1+2, vec![0, 1, 3]))); // 3
        assert_eq!(iter.next(), Some((2+2, vec![2, 1, 3]))); // 4
        assert_eq!(iter.next(), Some((3+2, vec![3, 1, 3]))); // 5
        assert_eq!(iter.next(), Some((1+5, vec![0, 1, 6]))); // 6
        assert_eq!(iter.next(), Some((4+2, vec![4, 1, 3]))); // 6
        assert_eq!(iter.next(), Some((2+5, vec![2, 1, 6]))); // 7
        assert_eq!(iter.next(), Some((3+5, vec![3, 1, 6]))); // 8
        assert_eq!(iter.next(), Some((4+5, vec![4, 1, 6]))); // 9
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn easy_best_proximity_for() {
        // classic
        assert_eq!(best_proximity_for(0, 0, &[0]),    Some((0, vec![0])));
        assert_eq!(best_proximity_for(0, 1, &[0]),    None);
        assert_eq!(best_proximity_for(1, 1, &[0]),    Some((2, vec![0])));
        assert_eq!(best_proximity_for(0, 1, &[0, 1]), Some((1, vec![1])));
        assert_eq!(best_proximity_for(1, 1, &[0, 2]), Some((1, vec![2])));
        assert_eq!(best_proximity_for(1, 2, &[0, 2]), Some((2, vec![0])));
        assert_eq!(best_proximity_for(1, 2, &[0, 3]), Some((2, vec![0, 3])));

        // limits
        assert_eq!(best_proximity_for(2, 7, &[0, 9]),   Some((7, vec![9])));
        assert_eq!(best_proximity_for(12, 7, &[6, 19]), Some((7, vec![6, 19])));

        // another attribute
        assert_eq!(best_proximity_for(1000, 7, &[994, 1007]), Some((7, vec![1007])));
        assert_eq!(best_proximity_for(1004, 7, &[994, 1011]), Some((7, vec![1011])));
        assert_eq!(best_proximity_for(1004, 8, &[900, 913, 1000, 1012, 2012]), Some((8, vec![900, 913, 1012, 2012])));
        assert_eq!(best_proximity_for(1009, 8, &[900, 913, 1002, 1012, 2012]), Some((8, vec![900, 913, 1002, 2012])));
    }
}
