use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::bpmn::{BpmnStep, ProcessName, State, storage::StorageBackend};
use crate::petri_net::{Id, Place};

use super::RegisteredProcess;

type PlaceMapping<B> = HashMap<Id<Place<State<<B as StorageBackend>::Storage>>>, BTreeSet<String>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BpmnNodeKind {
    ServiceTask,
    IntermediateCatchEvent,
    ExclusiveGateway,
    ParallelGateway,
}

#[derive(Debug, Clone)]
struct BpmnNode {
    id: String,
    name: Option<String>,
    kind: BpmnNodeKind,
}

fn escape_xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn sanitize_xml_id(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for (index, ch) in value.chars().enumerate() {
        let allowed = ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.');
        let mapped = if allowed { ch } else { '_' };
        if index == 0 && !(mapped.is_ascii_alphabetic() || mapped == '_') {
            result.push('_');
        }
        result.push(mapped);
    }

    if result.is_empty() {
        result.push_str("Process");
    }

    result
}

#[derive(Clone)]
struct Bounds {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

impl Bounds {
    fn cy(&self) -> f64 {
        self.y + self.h / 2.0
    }
}

fn element_size(node_id: &str, kind: Option<BpmnNodeKind>) -> (f64, f64) {
    if node_id.starts_with("StartEvent") || node_id.starts_with("EndEvent") {
        return (36.0, 36.0);
    }
    match kind {
        Some(BpmnNodeKind::ServiceTask) => (100.0, 80.0),
        Some(BpmnNodeKind::IntermediateCatchEvent) => (36.0, 36.0),
        Some(BpmnNodeKind::ExclusiveGateway) | Some(BpmnNodeKind::ParallelGateway) => (50.0, 50.0),
        None => (100.0, 80.0),
    }
}

/// Assigns x/y positions using a longest-path level assignment (Kahn's topological ordering).
/// Each column maps to one level; within a column nodes are centred vertically.
fn compute_layout(
    nodes: &[BpmnNode],
    start_id: &str,
    end_id: &str,
    sequence_flows: &[(String, String, String)],
) -> HashMap<String, Bounds> {
    let mut successors: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();

    let mut all_ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
    all_ids.push(start_id);
    all_ids.push(end_id);

    for &id in &all_ids {
        in_degree.entry(id.to_string()).or_insert(0);
    }
    for (_, src, tgt) in sequence_flows {
        successors
            .entry(src.as_str())
            .or_default()
            .push(tgt.as_str());
        *in_degree.entry(tgt.clone()).or_insert(0) += 1;
    }

    let mut levels: HashMap<String, usize> =
        all_ids.iter().map(|&id| (id.to_string(), 0)).collect();

    let mut queue: VecDeque<String> = all_ids
        .iter()
        .filter(|&&id| in_degree.get(id).copied().unwrap_or(0) == 0)
        .map(|&id| id.to_string())
        .collect();

    while let Some(node) = queue.pop_front() {
        let current_level = levels[&node];
        let succs: Vec<String> = successors
            .get(node.as_str())
            .map(|v| v.iter().map(|&s| s.to_string()).collect())
            .unwrap_or_default();
        for succ in succs {
            let new_level = current_level + 1;
            let level = levels.entry(succ.clone()).or_insert(0);
            if new_level > *level {
                *level = new_level;
            }
            let deg = in_degree.entry(succ.clone()).or_insert(1);
            *deg = deg.saturating_sub(1);
            if *deg == 0 {
                queue.push_back(succ);
            }
        }
    }

    let mut by_level: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, &level) in &levels {
        by_level.entry(level).or_default().push(id.clone());
    }
    for nodes_at in by_level.values_mut() {
        nodes_at.sort();
    }

    let kind_map: HashMap<&str, BpmnNodeKind> =
        nodes.iter().map(|n| (n.id.as_str(), n.kind)).collect();

    const X_ORIGIN: f64 = 150.0;
    const X_GAP: f64 = 200.0;
    const Y_CENTER: f64 = 200.0;
    const Y_GAP: f64 = 120.0;

    let mut bounds = HashMap::new();
    for (level, node_ids) in &by_level {
        let x_center = X_ORIGIN + (*level as f64) * X_GAP;
        let count = node_ids.len();
        for (i, id) in node_ids.iter().enumerate() {
            let y = Y_CENTER + ((i as f64) - (count as f64 - 1.0) / 2.0) * Y_GAP;
            let (w, h) = element_size(id, kind_map.get(id.as_str()).copied());
            bounds.insert(
                id.clone(),
                Bounds {
                    x: x_center - w / 2.0,
                    y: y - h / 2.0,
                    w,
                    h,
                },
            );
        }
    }

    bounds
}

pub(super) fn export<B: StorageBackend>(registered: &RegisteredProcess<B>) -> String {
    let process_name = ProcessName::from(&registered.meta_data).to_string();
    let process_id = format!("Process_{}", sanitize_xml_id(&process_name));
    let definitions_id = format!("Definitions_{}", sanitize_xml_id(&process_name));
    let start_event_id = "StartEvent_1".to_string();
    let end_event_id = "EndEvent_1".to_string();

    let mut nodes: Vec<BpmnNode> = Vec::new();
    let mut seen_node_ids: HashSet<String> = HashSet::new();
    let mut place_to_incoming: PlaceMapping<B> = HashMap::new();
    let mut place_to_outgoing: PlaceMapping<B> = HashMap::new();

    // Pass 1: classify transitions to detect gateway groupings.
    //
    // XOR split: multiple XorBranch transitions share the same input place → one gateway.
    // XOR join:  multiple Task transitions share the same step name (and each have 1 input arc)
    //            → one gateway.
    // AND join:  a single Task transition with >1 input arcs → parallel gateway.
    let mut xor_split_canonical: HashMap<Id<Place<State<B::Storage>>>, usize> = HashMap::new();
    let mut task_count: HashMap<String, usize> = HashMap::new();
    let mut task_first: HashMap<String, usize> = HashMap::new();

    for (i, transition) in registered.petri_net.transitions().enumerate() {
        match &transition.action {
            BpmnStep::XorBranch(_, _) => {
                if let Some(arc) = transition.input.first() {
                    xor_split_canonical.entry(arc.target).or_insert(i);
                }
            }
            BpmnStep::Task(step, _) if transition.input.len() == 1 => {
                let name = step.as_str().to_string();
                if name != "End" {
                    *task_count.entry(name.clone()).or_insert(0) += 1;
                    task_first.entry(name).or_insert(i);
                }
            }
            _ => {}
        }
    }

    // Pass 2: build deduplicated nodes and connectivity maps.
    // BTreeSet values auto-deduplicate when the same canonical ID is inserted multiple
    // times for the same place (XOR split input / XOR join output).
    for (i, transition) in registered.petri_net.transitions().enumerate() {
        let (canonical_id, kind, name) = match &transition.action {
            BpmnStep::XorBranch(step, _) => {
                let canonical_idx = transition
                    .input
                    .first()
                    .and_then(|a| xor_split_canonical.get(&a.target))
                    .copied()
                    .unwrap_or(i);
                let base_name = step
                    .as_str()
                    .rsplit_once(": ")
                    .map(|(base, _)| base.to_string())
                    .unwrap_or_else(|| step.as_str().to_string());
                (
                    format!("ExclusiveGateway_{canonical_idx}"),
                    BpmnNodeKind::ExclusiveGateway,
                    Some(base_name),
                )
            }
            BpmnStep::Task(step, _) if transition.input.len() > 1 => {
                // AND join: single Task transition with multiple input arcs.
                (
                    format!("ParallelGateway_{i}"),
                    BpmnNodeKind::ParallelGateway,
                    None,
                )
            }
            BpmnStep::Task(step, _) => {
                let name_str = step.as_str().to_string();
                if name_str == "End" {
                    // Transparent end step: wire input places directly to the end event.
                    for arc in &transition.input {
                        place_to_outgoing
                            .entry(arc.target)
                            .or_default()
                            .insert(end_event_id.clone());
                    }
                    continue;
                }
                if task_count.get(&name_str).copied().unwrap_or(0) > 1 {
                    // XOR join: multiple Task transitions share this step name.
                    let canonical_idx = task_first.get(&name_str).copied().unwrap_or(i);
                    (
                        format!("ExclusiveGateway_{canonical_idx}"),
                        BpmnNodeKind::ExclusiveGateway,
                        None,
                    )
                } else {
                    (
                        format!("Task_{i}"),
                        BpmnNodeKind::ServiceTask,
                        Some(name_str),
                    )
                }
            }
            BpmnStep::Waitable(step, _) => (
                format!("CatchEvent_{i}"),
                BpmnNodeKind::IntermediateCatchEvent,
                Some(step.as_str().to_string()),
            ),
            BpmnStep::And(_) => (
                format!("ParallelGateway_{i}"),
                BpmnNodeKind::ParallelGateway,
                None,
            ),
        };

        if seen_node_ids.insert(canonical_id.clone()) {
            nodes.push(BpmnNode {
                id: canonical_id.clone(),
                name,
                kind,
            });
        }

        for arc in &transition.input {
            place_to_outgoing
                .entry(arc.target)
                .or_default()
                .insert(canonical_id.clone());
        }
        for arc in &transition.output {
            place_to_incoming
                .entry(arc.target)
                .or_default()
                .insert(canonical_id.clone());
        }
    }

    place_to_incoming
        .entry(registered.start)
        .or_default()
        .insert(start_event_id.clone());
    place_to_outgoing
        .entry(registered.end)
        .or_default()
        .insert(end_event_id.clone());

    let mut places = BTreeSet::new();
    places.extend(place_to_incoming.keys().copied());
    places.extend(place_to_outgoing.keys().copied());

    let mut flow_pairs = BTreeSet::new();
    for place in places {
        let incoming = place_to_incoming.get(&place).cloned().unwrap_or_default();
        let outgoing = place_to_outgoing.get(&place).cloned().unwrap_or_default();
        for source in &incoming {
            for target in &outgoing {
                if source != target {
                    flow_pairs.insert((source.clone(), target.clone()));
                }
            }
        }
    }

    let mut incoming_by_node: HashMap<String, Vec<String>> = HashMap::new();
    let mut outgoing_by_node: HashMap<String, Vec<String>> = HashMap::new();
    let mut sequence_flows = Vec::new();

    for (index, (source, target)) in flow_pairs.into_iter().enumerate() {
        let flow_id = format!("Flow_{}", index + 1);
        incoming_by_node
            .entry(target.clone())
            .or_default()
            .push(flow_id.clone());
        outgoing_by_node
            .entry(source.clone())
            .or_default()
            .push(flow_id.clone());
        sequence_flows.push((flow_id, source, target));
    }

    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(&format!(
        "<bpmn:definitions id=\"{}\" targetNamespace=\"urn:tower-bpmn:{}\" xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\" xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\" xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\" xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\">\n",
        escape_xml(&definitions_id),
        escape_xml(&process_name)
    ));
    xml.push_str(&format!(
        "  <bpmn:process id=\"{}\" name=\"{}\" isExecutable=\"true\">\n",
        escape_xml(&process_id),
        escape_xml(registered.meta_data.name.as_ref())
    ));

    if let Some(description) = &registered.meta_data.description {
        xml.push_str(&format!(
            "    <bpmn:documentation>{}</bpmn:documentation>\n",
            escape_xml(description.as_ref())
        ));
    }

    xml.push_str(&format!("    <bpmn:startEvent id=\"{}\"", start_event_id));
    if let Some(outgoing) = outgoing_by_node.get(&start_event_id) {
        xml.push_str(">\n");
        for flow in outgoing {
            xml.push_str(&format!("      <bpmn:outgoing>{}</bpmn:outgoing>\n", flow));
        }
        xml.push_str("    </bpmn:startEvent>\n");
    } else {
        xml.push_str(" />\n");
    }

    for node in &nodes {
        let tag = match node.kind {
            BpmnNodeKind::ServiceTask => "bpmn:serviceTask",
            BpmnNodeKind::IntermediateCatchEvent => "bpmn:intermediateCatchEvent",
            BpmnNodeKind::ExclusiveGateway => "bpmn:exclusiveGateway",
            BpmnNodeKind::ParallelGateway => "bpmn:parallelGateway",
        };

        xml.push_str(&format!("    <{tag} id=\"{}\"", escape_xml(&node.id)));
        if let Some(name) = &node.name {
            xml.push_str(&format!(" name=\"{}\"", escape_xml(name)));
        }

        let incoming = incoming_by_node.get(&node.id);
        let outgoing = outgoing_by_node.get(&node.id);

        if incoming.is_none() && outgoing.is_none() {
            xml.push_str(" />\n");
            continue;
        }

        xml.push_str(">\n");
        if let Some(incoming) = incoming {
            for flow in incoming {
                xml.push_str(&format!("      <bpmn:incoming>{}</bpmn:incoming>\n", flow));
            }
        }
        if let Some(outgoing) = outgoing {
            for flow in outgoing {
                xml.push_str(&format!("      <bpmn:outgoing>{}</bpmn:outgoing>\n", flow));
            }
        }
        xml.push_str(&format!("    </{tag}>\n"));
    }

    xml.push_str(&format!("    <bpmn:endEvent id=\"{}\"", end_event_id));
    if let Some(incoming) = incoming_by_node.get(&end_event_id) {
        xml.push_str(">\n");
        for flow in incoming {
            xml.push_str(&format!("      <bpmn:incoming>{}</bpmn:incoming>\n", flow));
        }
        xml.push_str("    </bpmn:endEvent>\n");
    } else {
        xml.push_str(" />\n");
    }

    for (flow_id, source, target) in &sequence_flows {
        xml.push_str(&format!(
            "    <bpmn:sequenceFlow id=\"{}\" sourceRef=\"{}\" targetRef=\"{}\" />\n",
            escape_xml(flow_id),
            escape_xml(source),
            escape_xml(target)
        ));
    }

    xml.push_str("  </bpmn:process>\n");

    // BPMNDI visualization
    let layout = compute_layout(&nodes, &start_event_id, &end_event_id, &sequence_flows);

    xml.push_str(&format!(
        "  <bpmndi:BPMNDiagram id=\"BPMNDiagram_{}\">\n",
        escape_xml(&process_id)
    ));
    xml.push_str(&format!(
        "    <bpmndi:BPMNPlane id=\"BPMNPlane_{}\" bpmnElement=\"{}\">\n",
        escape_xml(&process_id),
        escape_xml(&process_id)
    ));

    if let Some(b) = layout.get(&start_event_id) {
        xml.push_str(&format!(
            "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\">\n        <dc:Bounds x=\"{1:.0}\" y=\"{2:.0}\" width=\"{3:.0}\" height=\"{4:.0}\" />\n      </bpmndi:BPMNShape>\n",
            start_event_id, b.x, b.y, b.w, b.h
        ));
    }

    for node in &nodes {
        if let Some(b) = layout.get(&node.id) {
            let marker = if matches!(node.kind, BpmnNodeKind::ExclusiveGateway) {
                " isMarkerVisible=\"true\""
            } else {
                ""
            };
            xml.push_str(&format!(
                "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\"{1}>\n        <dc:Bounds x=\"{2:.0}\" y=\"{3:.0}\" width=\"{4:.0}\" height=\"{5:.0}\" />\n      </bpmndi:BPMNShape>\n",
                escape_xml(&node.id), marker, b.x, b.y, b.w, b.h
            ));
        }
    }

    if let Some(b) = layout.get(&end_event_id) {
        xml.push_str(&format!(
            "      <bpmndi:BPMNShape id=\"{0}_di\" bpmnElement=\"{0}\">\n        <dc:Bounds x=\"{1:.0}\" y=\"{2:.0}\" width=\"{3:.0}\" height=\"{4:.0}\" />\n      </bpmndi:BPMNShape>\n",
            end_event_id, b.x, b.y, b.w, b.h
        ));
    }

    for (flow_id, source, target) in &sequence_flows {
        let (sx, sy) = layout
            .get(source.as_str())
            .map(|b| (b.x + b.w, b.cy()))
            .unwrap_or((0.0, 0.0));
        let (tx, ty) = layout
            .get(target.as_str())
            .map(|b| (b.x, b.cy()))
            .unwrap_or((0.0, 0.0));
        xml.push_str(&format!(
            "      <bpmndi:BPMNEdge id=\"{0}_di\" bpmnElement=\"{0}\">\n        <di:waypoint x=\"{1:.0}\" y=\"{2:.0}\" />\n        <di:waypoint x=\"{3:.0}\" y=\"{4:.0}\" />\n      </bpmndi:BPMNEdge>\n",
            escape_xml(flow_id), sx, sy, tx, ty
        ));
    }

    xml.push_str("    </bpmndi:BPMNPlane>\n");
    xml.push_str("  </bpmndi:BPMNDiagram>\n");
    xml.push_str("</bpmn:definitions>\n");
    xml
}

#[cfg(test)]
mod tests {
    use crate::bpmn::{MetaData, Process, ProcessBuilder, Runtime, gateways, storage::InMemory};
    use crate::executor::TokioExecutor;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct XmlExportProcess;

    impl Process for XmlExportProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData = MetaData {
                name: std::borrow::Cow::Borrowed("xml-export"),
                version: 1,
                description: Some(std::borrow::Cow::Borrowed("Export <BPMN> & verify")),
            };
            &META
        }

        fn define<S: crate::bpmn::storage::Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            builder
                .then("first step", |_token, input| input + 1)
                .then("second & final", |_token, input| input)
        }
    }

    #[test]
    fn bpmn_export_contains_required_bpmn_2_0_structure() {
        let mut runtime: Runtime<TokioExecutor, InMemory> = Runtime::default();
        runtime
            .register_process(XmlExportProcess)
            .expect("process registration should succeed");

        let process = runtime
            .registered_processes()
            .next()
            .expect("process should be registered");

        let xml = process.bpmn();

        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<bpmn:definitions "));
        assert!(xml.contains("xmlns:bpmn=\"http://www.omg.org/spec/BPMN/20100524/MODEL\""));
        assert!(xml.contains("<bpmn:process "));
        assert!(xml.contains("<bpmn:startEvent id=\"StartEvent_1\""));
        assert!(xml.contains("<bpmn:endEvent id=\"EndEvent_1\""));
        assert!(xml.contains("<bpmn:serviceTask id=\"Task_0\" name=\"first step\""));
        assert!(xml.contains("<bpmn:serviceTask id=\"Task_1\" name=\"second &amp; final\""));
        assert!(xml.contains("<bpmn:sequenceFlow id=\"Flow_1\""));
        assert!(
            xml.contains(
                "<bpmn:documentation>Export &lt;BPMN&gt; &amp; verify</bpmn:documentation>"
            )
        );
        assert!(xml.trim_end().ends_with("</bpmn:definitions>"));

        assert!(xml.contains("xmlns:bpmndi=\"http://www.omg.org/spec/BPMN/20100524/DI\""));
        assert!(xml.contains("xmlns:dc=\"http://www.omg.org/spec/DD/20100524/DC\""));
        assert!(xml.contains("xmlns:di=\"http://www.omg.org/spec/DD/20100524/DI\""));
        assert!(xml.contains("<bpmndi:BPMNDiagram "));
        assert!(xml.contains("<bpmndi:BPMNPlane "));
        assert!(xml.contains("<bpmndi:BPMNShape id=\"StartEvent_1_di\""));
        assert!(xml.contains("<bpmndi:BPMNShape id=\"EndEvent_1_di\""));
        assert!(xml.contains("<bpmndi:BPMNShape id=\"Task_0_di\""));
        assert!(xml.contains("<dc:Bounds "));
        assert!(xml.contains("<bpmndi:BPMNEdge "));
        assert!(xml.contains("<di:waypoint "));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct XmlGatewayProcess;

    impl Process for XmlGatewayProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData = MetaData::new("xml-gateways", "Gateway XML coverage");
            &META
        }

        fn define<S: crate::bpmn::storage::Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            let [left, right] =
                builder
                    .then("prepare", |_token, value| value)
                    .split(gateways::Xor::for_splitting(
                        "Modulo",
                        |_token, value: i32| {
                            if value % 2 == 0 { 0 } else { 1 }
                        },
                    ));

            ProcessBuilder::join(
                gateways::Xor::for_joining("One branch done"),
                [
                    left.then("left", |_token, value| value + 1),
                    right.then("right", |_token, value| value + 2),
                ],
            )
        }
    }

    #[test]
    fn bpmn_export_contains_gateway_elements() {
        let mut runtime: Runtime<TokioExecutor, InMemory> = Runtime::default();
        runtime
            .register_process(XmlGatewayProcess)
            .expect("process registration should succeed");

        let process = runtime
            .registered_processes()
            .find(|registered| registered.meta_data.name == "xml-gateways")
            .expect("gateway process should be registered");

        let xml = process.bpmn();

        assert!(xml.contains("<bpmn:exclusiveGateway id=\"ExclusiveGateway_"));
        assert!(xml.contains("name=\"Modulo\""));
        assert!(!xml.contains("name=\"One branch done\""));
        assert!(xml.contains("<bpmn:incoming>Flow_"));
        assert!(xml.contains("<bpmn:outgoing>Flow_"));
    }
}
