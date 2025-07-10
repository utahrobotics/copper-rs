use ratatui::layout::Position;

use super::*;

const MARGIN: u16 = 5;

#[derive(Debug)]
pub struct NodeGraph<'a> {
	nodes: Vec<NodeLayout<'a>>,
	connections: Vec<Connection>,
	placements: Map<usize, Rect>,
	pub conn_layout: ConnectionsLayout,
	width: usize,
}

impl<'a> NodeGraph<'a> {
	pub fn new(
		nodes: Vec<NodeLayout<'a>>,
		connections: Vec<Connection>,
		width: usize,
		height: usize,
	) -> Self {
		Self {
			nodes,
			connections,
			conn_layout: ConnectionsLayout::new(width, height),
			placements: Default::default(),
			width,
		}
	}

	pub fn calculate(&mut self) {
		self.placements.clear();

		// find root nodes
		let mut roots: Set<_> = (0..self.nodes.len()).collect();
		for ea_connection in self.connections.iter() {
			roots.remove(&ea_connection.from_node);
		}

		// place them and their children (recursively)
		let mut main_chain = Vec::new();
		for ea_root in roots {
			self.place_node(ea_root, 0, 0, &mut main_chain);
			assert!(main_chain.is_empty());
		}

		// calculate connections (eventually, this should be done during node
		// placement, but thats really complicated and i dont wanna deal with that
		// right now. essentially, adding non-trivial connections nudges nodes,
		// and nudging nodes nudges existing connections.)
		let mut conn_map = Map::<(usize, usize), usize>::new();
		let mut next_idx = 1;
		for ea_conn in self.connections.iter() {
			let a_pos = self.placements[&ea_conn.from_node];
			let b_pos = self.placements[&ea_conn.to_node];
			// NOTE: don't forget that left and right are swapped
			let a_point = (
				self.width.saturating_sub(a_pos.left().into()),
				a_pos.top() as usize + ea_conn.from_port + 1,
			);
			let b_point = (
				self.width.saturating_sub(b_pos.right() as usize + 1),
				b_pos.top() as usize + ea_conn.to_port + 1,
			);
			self.conn_layout.insert_port(
				false,
				ea_conn.from_node,
				ea_conn.from_port,
				a_point,
			);
			self.conn_layout.insert_port(true, ea_conn.to_node, ea_conn.to_port, b_point);
			let key = (ea_conn.from_node, ea_conn.from_port);
			if !conn_map.contains_key(&key) {
				conn_map.insert(key, next_idx);
				next_idx += 1;
			}
			self.conn_layout.push_connection((*ea_conn, conn_map[&key]));
			self.conn_layout.block_port(a_point);
			self.conn_layout.block_port(b_point);
		}
		for mut ea_placement in self.placements.values().cloned() {
			ea_placement.x =
				(self.width as u16).saturating_sub(ea_placement.x + ea_placement.width);
			self.conn_layout.block_zone(ea_placement);
		}
		self.conn_layout.calculate();
	}

	/// ATTENTION: x_offs works in the opposite direction (higher values are
	/// further left) and y_offs is the same as tui (higher values are further
	/// down)
	fn place_node(
		&mut self,
		idx_node: usize,
		x: u16,
		y: u16,
		main_chain: &mut Vec<usize>,
	) {
		// place the node
		let size_me = self.nodes[idx_node].size;
		let mut rect_me = Rect { x, y, width: size_me.0, height: size_me.1 };

		// nudge placement. if a node intersects with another node, its entire
		// main chain (largest subset of nodes including this one where every
		// node is the first child of its parent) should be moved down to not
		// intersect.
		//
		// Repeat the for loop until in runs all the way through without any
		// intersections. Surely there's a more efficient way to do this.
		'outer: loop {
			for (_, ea_them) in self.placements.iter() {
				if rect_me.intersects(*ea_them) {
					rect_me.y = rect_me.y.max(ea_them.bottom());
					continue 'outer;
				}
			}
			break;
		}
		for ea_node in main_chain.iter() {
			let y = self.placements[ea_node].y.max(rect_me.y);
			self.placements.get_mut(ea_node).unwrap().y = y;
		}
		self.placements.insert(idx_node, rect_me);

		// find children and order them
		let mut y = y;
		main_chain.push(idx_node);
		for ea_child in get_upstream(&self.connections, idx_node) {
			if self.placements.contains_key(&ea_child.from_node) {
				// nudge it (if necessary)
				self.nudge(ea_child.from_node, rect_me.x + rect_me.width + MARGIN);
			} else {
				// place it
				self.place_node(
					ea_child.from_node,
					x + rect_me.width + MARGIN,
					y,
					main_chain,
				);
				main_chain.clear();
				y += self.placements[&ea_child.from_node].height;
			}
		}
		main_chain.pop();
	}

	fn nudge(&mut self, idx_node: usize, x: u16) {
		let rect_me = self.placements[&idx_node];
		if rect_me.x < x {
			self.placements.get_mut(&idx_node).unwrap().x = x;
			for ea_child in get_upstream(&self.connections, idx_node) {
				assert!(self.placements.contains_key(&ea_child.from_node));
				self.nudge(ea_child.from_node, x + rect_me.width + MARGIN);
			}
		}
	}

	pub fn split(&self, area: Rect) -> Vec<Rect> {
		(0..self.nodes.len())
			.map(|idx_node| {
				self.placements
					.get(&idx_node)
					.map(|pos| {
						if pos.right() > area.width || pos.bottom() > area.height {
							return Rect { x: 0, y: 0, width: 0, height: 0 };
						}
						let mut pos = *pos;
						pos.x = area.width - pos.right() + area.x;
						pos.y += area.y;
						pos.inner(Margin { horizontal: 1, vertical: 1 })
					})
					.unwrap_or_default()
			})
			.collect()
	}
}

fn get_upstream(conns: &[Connection], idx_node: usize) -> Vec<Connection> {
	// find children and order them
	let mut upstream: Vec<_> =
		conns.iter().filter(|ea| ea.to_node == idx_node).copied().collect();
	upstream.sort_by(|a, b| a.to_port.cmp(&b.to_port));
	upstream
}

fn get_downstream(conns: &[Connection], idx_node: usize) -> Vec<Connection> {
	// find parents and order them
	let mut downstream: Vec<_> =
		conns.iter().filter(|ea| ea.from_node == idx_node).copied().collect();
	downstream.sort_by(|a, b| a.from_port.cmp(&b.from_port));
	downstream
}

impl<'a> ratatui::widgets::StatefulWidget for NodeGraph<'a> {
	// eventually, this will contain stuff like view position
	//	type State = NodeGraphState;
	type State = ();

	fn render(self, area: Rect, buf: &mut Buffer, _state: &mut Self::State) {
		// draw connections
		self.conn_layout.render(area, buf);

		// draw nodes
		'node: for (idx_node, ea_node) in self.nodes.into_iter().enumerate() {
			if let Some(mut pos) = self.placements.get(&idx_node).copied() {
				if pos.right() > area.width || pos.bottom() > area.height {
					continue 'node;
				}
				// draw box
				pos.x = area.left() + area.width - pos.right();
				pos.y += area.top();
				let block = ea_node.block();
				block.render(pos, buf);
				// draw connection ports
				for ea_conn in get_upstream(&self.connections, idx_node) {
					// draw connection alias
					if let Some(alias_char) = self.conn_layout.alias_connections.get(&(
						true,
						idx_node,
						ea_conn.to_port,
					)) {
						let y = pos.top() + ea_conn.to_port as u16 + 1;
						if pos.left() > 0 && y < area.width {
							buf.cell_mut(Position::new(pos.left() - 1, y))
								.unwrap()
								.set_symbol(alias_char)
								.set_style(
									Style::default()
										.add_modifier(Modifier::BOLD)
										.bg(Color::Red),
								);
						}
					}

					// draw port
					buf.cell_mut(Position::new(
						pos.left(),
						pos.top() + ea_conn.to_port as u16 + 1,
					))
					.unwrap()
					.set_symbol(conn_symbol(
						true,
						ea_node.border_type(),
						ea_conn.line_type(),
					));
				}
				for ea_conn in get_downstream(&self.connections, idx_node) {
					// draw connection alias
					if let Some(alias_char) = self.conn_layout.alias_connections.get(&(
						false,
						idx_node,
						ea_conn.from_port,
					)) {
						buf.cell_mut(Position::new(
							pos.right(),
							pos.top() + ea_conn.from_port as u16 + 1,
						))
						.unwrap()
						.set_symbol(alias_char)
						.set_style(
							Style::default().add_modifier(Modifier::BOLD).bg(Color::Red),
						);
					}

					// draw port
					buf.cell_mut(Position::new(
						pos.right() - 1,
						pos.top() + ea_conn.from_port as u16 + 1,
					))
					.unwrap()
					.set_symbol(conn_symbol(
						false,
						ea_node.border_type(),
						ea_conn.line_type(),
					));
				}
			} else {
				buf.set_string(
					0,
					idx_node as u16,
					format!("{idx_node}"),
					Style::default(),
				);
			}
		}
	}
}
