import os
import csv
import json


def get_attribute_object(blob, atrribute_type_id) -> dict:
    for obj in blob:
        if obj['attribute_type_id'] == atrribute_type_id:
            return obj


def get_attribute_list(blob, attribute_type_id) -> list:
    object_list = []
    for obj in blob:
        if obj['attribute_type_id'] == attribute_type_id:
            object_list.append(obj)
    return object_list


def load_nodes(file_path) -> dict:
    nodes_data = {}
    with open(file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for row in reader:
            nodes_data[row[0]] = (row[1], row[2])
    return nodes_data


def load_data(data_folder):
    entity_dict = load_nodes(os.path.join(data_folder, "cooccurrence_nodes.tsv"))
    edges_file_path = os.path.join(data_folder, "cooccurrence_edges.tsv")
    # this is a list of ID types where we want the prefix included
    prefix_list = ['RHEA', 'GO', 'CHEBI', 'HP', 'MONDO', 'DOID', 'EFO', 'UBERON', 'MP', 'CL', 'MGI']
    with open(edges_file_path, 'r') as file_handle:
        reader = csv.reader(file_handle, delimiter='\t')
        for line in reader:
            subject_parts = line[0].split(':')
            object_parts = line[2].split(':')
            if line[0] not in entity_dict or line[2] not in entity_dict:
                continue
            yield {
                "_id": line[3],
                "subject": {
                    "id": line[0],
                    subject_parts[0]: line[0] if subject_parts[0] in prefix_list else subject_parts[1],
                    "type": entity_dict[line[0]][1].split(':')[-1]
                },
                "association": {
                    "edge_label": line[1].split(':')[-1],
                    "edge_attributes": json.loads(line[-1], parse_float=str, parse_int=str)
                },
                "object": {
                    "id": line[2],
                    object_parts[0]: line[2] if object_parts[0] in prefix_list else object_parts[1],
                    "type": entity_dict[line[2]][1].split(':')[-1]
                },
            }
