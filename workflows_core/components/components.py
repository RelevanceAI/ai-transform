"""
    All components are here
"""
from dataclasses import dataclass, asdict, field
from typing import Any, List

COLAB_PREFIX = (
    "https://colab.research.google.com/github/RelevanceAI/workflows/blob/main/"
)

# This is useful if you are ignoring
GENERATED_TYPES = ["sentiment", "cluster", "tag", "emotion"]


@dataclass
class Component:
    """
    Base class for all components
    """

    optional: bool = True
    default_value: Any = None
    doc: dict = field(default_factory=lambda: {"props": {}})
    # hooks are a way to modify the document
    only_types: list = None
    exclude_types: list = None

    def _add_optional(self, doc: dict):
        if self.optional:
            doc["props"]["optional"] = self.optional
        if "optional" in doc:
            doc.pop("optional")
        return doc

    def _add_multiple(self, doc: dict):
        if self.multiple:
            doc["props"]["multiple"] = self.multiple
        if "multiple" in doc:
            doc.pop("multiple")
        return doc

    def _add_default_value(self, doc: dict):
        if self.default_value is not None:
            doc["props"]["value"] = self.default_value
        # remove the default value nonsense
        if "default_value" in doc:
            doc.pop("default_value")
        return doc

    def _add_exclude_types(self, doc: dict):
        if self.exclude_types:
            doc["props"]["excludeType"] = self.exclude_types
        if "exclude_types" in doc:
            doc.pop("exclude_types")
        return doc

    def _add_only_types(self, doc: dict):
        if self.only_types:
            doc["props"]["onlyTypes"] = self.only_types
        if "only_types" in doc:
            doc.pop("only_types")
        return doc

    @property
    def hooks(self) -> list:
        return [self._add_optional]

    def json(self):
        doc = asdict(self)
        if "doc" in doc:
            doc.pop("doc")
        if "props" not in doc:
            doc["props"] = {}
        for hook in self.hooks:
            doc = hook(doc)
        doc["valueKey"] = doc.pop("value_key")
        return doc


@dataclass
class FieldSelector(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "fieldSelector"
    only_types: list = None
    exclude_types: list = None
    multiple: bool = False

    def _add_only_types(self, doc: dict):
        doc["props"]["onlyTypes"] = self.only_types
        return doc

    @property
    def hooks(self):
        return [
            self._add_optional,
            self._add_only_types,
            self._add_multiple,
            self._add_exclude_types,
        ]


@dataclass
class FileUpload(Component):
    title: str = "Upload your files."
    description: str = "You can upload your files here."
    value_key: str = "images"
    type: str = "fileUpload"
    accept: str = "*"
    multiple: bool = True

    def _add_file_upload_props(self, doc: dict):
        doc["props"]["accept"] = self.accept
        doc["props"]["multiple"] = self.multiple
        if "accept" in doc:
            doc.pop("accept")
        if "multiple" in doc:
            doc.pop("multiple")
        return doc

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

    @property
    def hooks(self) -> list:
        return [self._add_optional, self._add_default_value]

    def _add_default_value(self, doc):
        doc["props"]["type"] = self.data_type
        doc["props"]["value"] = self.default_value
        return doc


@dataclass
class Option:
    label: str
    value: Any
    # description: str


@dataclass
class BaseDropdown(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    options: List[Option] = field(
        default_factory=lambda x: [
            Option(label="Yes", value=True),
            Option(label="No", value=False),
        ]
    )
    type: str = "baseDropdown"


@dataclass
class BoolDropdown(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    type: str = "baseDropdown"
    options: list = field(
        default_factory=lambda x: [
            {
                "label": "Yes",
                "value": True,
            },
            {
                "label": "No",
                "value": False,
            },
        ],
    )

    def _add_options(self):
        self.doc["props"]["options"] = self.options

    @property
    def hooks(self):
        return [
            self._add_optional,
            self._add_multiple,
            self._add_options,
            self._add_default_value,
        ]

    props: dict = field(
        default_factory=lambda: {"multiple": False, "optional": True, "value": True}
    )


@dataclass
class TagsInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = "tagsToDelete"
    separators: list = field(default_factory=lambda x: {"separators": [","]})
    type: str = "tagsInput"

    def _add_separators(self):
        self.doc["props"]["separators"] = [","]

    @property
    def hooks(self):
        return [self._add_optional, self._add_separators]


@dataclass
class SliderInput(Component):
    title: str = ""
    description: str = ""
    value_key: str = ""
    max: int = 100
    step: int = 1
    min: int = 1
    type: str = "baseInput"

    def _add_type(self, doc):
        doc["props"]["type"] = "number"
        return doc

    def _add_max(self, doc: dict):
        doc["props"]["max"] = self.max
        if "max" in doc:
            doc.pop("max")
        return doc

    def _add_min(self, doc: dict):
        doc["props"]["min"] = self.min
        if "min" in doc:
            doc.pop("min")
        return doc

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

    def _add_template(self, doc: dict):
        # Optional, defaults to "This will be stored as:",
        doc["props"]["outputLabel"] = self.output_label
        # Required, use `{ value }` to reference current component's value. Can reference other fields by `value_key`
        doc["props"]["template"] = self.template
        if "output_label" in doc:
            doc.pop("output_label")
        if "template" in doc:
            doc.pop("template")
        return doc

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

    def _add_aggregation_query(self, doc: dict):
        doc["props"]["aggregationQuery"] = self.aggregation_query
        if "aggregation_query" in doc:
            doc.pop("aggregation_query")
        return doc

    def _add_aggregation_result_field(self, doc: dict):
        doc["props"]["aggregationResultField"] = self.aggregation_result_field
        if "aggregation_result_field" in doc:
            doc.pop("aggregation_result_field")
        return doc

    def _add_max_run_results(self, doc: dict):
        doc["props"]["maxRunResults"] = self.max_run_results
        if "max_run_results" in doc:
            doc.pop("max_run_results")
        return doc

    @property
    def hooks(self):
        return [
            self._add_aggregation_query,
            self._add_aggregation_result_field,
            self._add_max_run_results,
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
    multiple: bool = True

    def _add_tag_text(self, doc: dict):
        doc["props"]["addTagText"] = self.add_tag_text
        if "add_tag_text" in doc:
            doc.pop("add_tag_text")
        return doc

    @property
    def hooks(self):
        return [self._add_optional, self._add_tag_text]


@dataclass
class DatasetInput(Component):
    title: str = "What do you want to call your dataset?"
    description: str = "Do not include spaces or capital letters or full stops."
    value_key: str = "dataset_input"
    type: str = "datasetNameInput"


@dataclass
class EmailDropdown(Component):
    title: str = (
        "Do you want to receive an notification email when the workflow is completed?"
    )
    description: str = "If you choose yes, then we will send you an email when the workflow is successfully completed."
    value_key: str = "send_email"
    type: str = "baseDropdown"
    props: dict = field(
        default_factory=lambda: {
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
            "value": True,
        }
    )


@dataclass
class RefreshComponent(Component):
    title: str = "Do you want to refresh?"
    description: str = "If you choose False, then we will run it on new rows only. If True, we will run it on the whole dataset."
    value_key: str = "refresh"
    type: str = "baseDropdown"
    props: dict = field(
        default_factory=lambda: {
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
            "value": False,
        }
    )
