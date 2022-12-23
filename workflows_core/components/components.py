"""
    All components are here
"""
from dataclasses import dataclass, asdict, field
import re
from typing import Any
from enum import Enum

COLAB_PREFIX = (
    "https://colab.research.google.com/github/RelevanceAI/workflows/blob/main/"
)

# This is useful if you are ignoring
GENERATED_TYPES = [
    "sentiment",
    "cluster",
    "tag",
    "emotion"
]

class DataTypes(Enum):
    VECTOR: str = "vector"

@dataclass
class Component:
    """
    Base class for all components
    """
    optional: bool = True
    default_value: Any = None
    doc: dict = field(default_factory= lambda: {'props': {}})
    # hooks are a way to modify the document
    only_types: list = None

    def _add_optional(self):
        self.doc['props']['optional'] = self.optional

    def _add_multiple(self):
        if self.multiple:
            self.doc['props']['multiple'] = self.multiple

    def _add_default_value(self):
        if self.default_value is not None:
            self.doc['props']['value'] = self.default_value
    
    @property
    def hooks(self) -> list:
        return [self._add_optional]

    def json(self):
        self.doc = asdict(self)
        if 'props' not in self.doc:
            self.doc['props'] = {}        
        for hook in self.hooks:
            hook()
        self.doc['valueKey'] = self.doc.pop("value_key")
        return self.doc

@dataclass
class FieldSelector(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "fieldSelector"
    only_types: list = None
    multiple: bool = False

    def _add_only_types(self):
        self.doc['props']['onlyTypes'] = self.only_types
    
    @property
    def hooks(self):
        return [self._add_optional, self._add_only_types, self._add_multiple]


@dataclass
class FileUpload(Component):
    title: str = "Upload your files."
    description: str = "You can upload your files here."
    value_key: str = "images"
    type: str = "fileUpload"
    accept: str = "*"
    multiple: bool = True
    def _add_file_upload_props(self):
        self.doc['props']['accept'] = self.accept
        self.doc['props']['multiple'] = self.multiple

    @property
    def hooks(self):
        return [self._add_optional, self._add_file_upload_props]


@dataclass
class AdvancedFilters(Component):
    title: str = "Add some advanced filters"
    description: str = "Here's where you can add advanced filters."
    value_key: str = "filters"
    type: str = "advancedFilterInput"

@dataclass
class BaseInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "baseInput"
    data_type: str = "text"
    default_value: str = "default"
    def _add_default_value(self):
        self.doc['props']['type'] = self.data_type
        self.doc['props']['value'] = self.default_value

@dataclass
class BaseDropdown(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "baseDropdown"

@dataclass
class BoolDropdown(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "baseDropdown"
    options: list = field(default_factory=lambda x: [
            {
                "label": "Yes",
                "value": True,
            },
            {
                "label": "No",
                "value": False,
            }
        ],
    )

    def _add_options(self):
        self.doc['props']['options'] = self.options
    
    @property
    def hooks(self):
        return [
            self._add_optional,
            self._add_multiple,
            self._add_options,
            self._add_default_value
        ]
    props: dict = field(default_factory=lambda: {
        "multiple": False,
        "optional": True,
        "value": True
    })


@dataclass
class TagsInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = "tagsToDelete"
    separators: list = field(default_factory=lambda x: {"separators": [","]})
    type: str = "tagsInput"

    def _add_separators(self):
        self.doc['props']['separators'] = [","]
    
    @property
    def hooks(self):
        return [
           self._add_optional, self._add_separators
        ]


@dataclass
class SliderInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    max: int = 100
    step: int = 1
    min: int = 1
    type: str = "baseInput"
    
    def _add_type(self):
        self.doc['props']['type'] = "number"
    
    def _add_max(self):
        self.doc['props']['max'] = self.max
    
    def _add_min(self):
        self.doc['props']['min'] = self.min

    @property
    def hooks(self):
        return [
            self._add_type,
            self._add_default_value,
            self._add_max,
            self._add_min,
            self._add_optional,
        ]


@dataclass
class DynamicTextInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "dynamicInput"
    output_label: str = "output field name:"
    template: str = "_surveytag_.{ vector_fields }.{ value }"
    def _add_template(self):
        # Optional, defaults to "This will be stored as:",
        self.doc['props']['outputLabel'] = self.output_label
        # Required, use `{ value }` to reference current component's value. Can reference other fields by `value_key`
        self.doc['props']['template'] = self.template
    
    @property
    def hooks(self):
        return [self._add_template, self._add_optional]


@dataclass
class AggregateTagsSelector(Component):
    """
    "aggregationQuery": {  # Required
        "groupby": [
            {
                "name": "field",  #
                "field": "{ field }",  # Use this to refer to the selected field
                "agg": "category",
            }
        ]
    },
    "aggregationResultField": "field",  # Required, sHould match `name` in aggregationQuery
    "maxRunResults": 20,  # maximum number of tags to get from aggregate, defaults to 20
    """
    title: str = "Add some tags"
    description: str = "Here's where you can add some tags."
    type: str = "aggregateTagsSelector"
    value_key: str = "sub_tags"
    aggregation_query: str = None
    aggregation_result_field: str = "field"
    max_run_results: int = 20
    props: dict = field(
        default_factory=lambda: {
            "aggregationQuery": {  # Required
                "groupby": [
                    {
                        "name": "field",  #
                        "field": "{ field }",  # Use this to refer to the selected field
                        "agg": "category",
                    }
                ]
            },
            "aggregationResultField": "field",  # Required, sHould match `name` in aggregationQuery
            "maxRunResults": 20,  # maximum number of tags to get from aggregate, defaults to 20
        }
    )
    def _add_aggregation_query(self):
        self.doc['props']['aggregationQuery'] = self.aggregation_query
    
    def _add_aggregation_result_field(self):
        self.doc['props']['aggregationResultField'] = self.aggregation_result_field
    
    def _add_max_run_results(self):
        self.doc['props']['maxRunResults'] = self.max_run_results
    
    @property
    def hooks(self):
        return [
            self._add_aggregation_query,
            self._add_aggregation_result_field,
            self._add_max_run_results
        ]

@dataclass
class AggregateSelector(Component):
    title: str = "Add some tags"
    description: str = "Here's where you can add some tags."
    type: str = "aggregateSelector"
    value_key: str = "sub_tags"
    props: dict = field(
        default_factory=lambda: {
            "aggregationQuery": {  # Required
                "groupby": [
                    {
                        "name": "field",  #
                        "field": "{ field }",  # Use this to refer to the selected field
                        "agg": "category",
                    }
                ]
            },
            "aggregationResultField": "field",  # Required, sHould match `name` in aggregationQuery
            "maxRunResults": 20,  # maximum number of tags to get from aggregate, defaults to 20
        }
    )


@dataclass
class ExplorerSelector(Component):
    title: str = "Select an explorer dashboard"
    description: str = "All the available explorer dashboards go here."
    value_key: str = "explorer_id"
    type: str = "explorerSelector"

@dataclass
class TagPairInput(Component):
    title: str = "Tags to merge"
    description: str = "Please put your tags to merge here."
    value_key: str = "tagsToMerge"
    add_tag_text: str = "Add new tags"
    type: str = "tagPairInput"

    def _add_tag_text(self):
        self.doc['props']['addTagText'] = self.add_tag_text

    @property
    def hooks(self):
        return [
            self._add_optional,
            self._add_tag_text
        ]

@dataclass
class DatasetInput(Component):
    title: str = "What do you want to call your dataset?"
    description: str = "Do not include spaces or capital letters or full stops."
    value_key: str = "dataset_input"
    type: str = "datasetNameInput"


@dataclass
class EmailDropdown(Component):
    title: str = "Do you want to receive an notification email when the workflow is completed?"
    description: str = "If you choose yes, then we will send you an email when the workflow is successfully completed."
    value_key: str = "send_email"
    type: str = "baseDropdown"
    props: dict = field(default_factory=lambda: {
        "options": [
            {
                "label": "Yes",
                "value": True,
            },
            {
                "label": "No",
                "value": False,
            },
        ],
        "multiple": False,
        "optional": True,
        "value": True
    })

@dataclass
class RefreshComponent(Component):
    title: str = "Do you want to refresh?"
    description: str = "If you choose False, then we will run it on new rows only. If True, we will run it on the whole dataset."
    value_key: str = "refresh"
    type: str = "baseDropdown"
    props: dict = field(default_factory=lambda: {
        "options": [
            {
                "label": "Yes",
                "value": True,
            },
            {
                "label": "No",
                "value": False,
            },
        ],
        "multiple": False,
        "optional": True,
        "value": False
    })
