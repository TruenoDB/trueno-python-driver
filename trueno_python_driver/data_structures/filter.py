from enum import Enum
import logging

__author__ = 'Miguel Rivera'
__version__ = '0.1.0'


class FilterType(Enum):
    """
    Filter Type Enumerator Class
     """
    And = 'AND'
    Or = 'OR'
    Not = 'NOT'


class FilterOperator(Enum):
    """
    Filter Type Enumerator Class
    """
    GreaterThan = 'gt'
    GreaterOrEqual = 'gte'
    LessThan = 'lt'
    LessOrEqual = 'lte'


class Filter:
    """
    Filters that are applicable to a Graph search operation.
    """
    def __init__(self):
        """
        Create a new Filter instance.
        """
        self.__logger = logging.getLogger(__name__)

        self.filters = []
        self.ftr = FilterType.And

    def term(self, prop, value):
        """
        Term matching filter, can be either a exact string or number.
        :param prop: The property/meta/computed to be applied on the operation.
        :param value: Value for this filter
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'term', 'prop': prop, 'val': value, 'ftr': self.ftr}
        self.filters.append(ftr)

        return self

    def range(self, prop, op, value):
        """
        Filter components by a specific range in their values
        :param prop: The property/meta/computed to be applied on the operation.
        :param op: FilterOperator value
        :param value: Value for this filter
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'range', 'prop': prop, 'op': op, 'val': value, 'ftr': self.ftr}
        self.filters.append(ftr)

        return self

    def exist(self, prop):
        """

        :param prop: The property/meta/computed to be applied on the operation.
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'exist', 'prop': prop, 'ftr': self.ftr}
        self.filters.append(ftr)

        return self

    def wildcard(self, prop, value):
        """

        :param prop: The property/meta/computed to be applied on the operation.
        :param value: Value for this filter
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'wildcard', 'prop': prop, 'val': value, 'ftr': self.ftr}
        self.filters.append(ftr)

        return self

    def regexp(self, prop, value):
        """

        :param prop: The property/meta/computed to be applied on the operation.
        :param value: Value for this filter
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'wildcard', 'prop': prop, 'val': value, 'ftr': self.ftr}
        self.filters.append(ftr)

        return self

    def limit(self, value):
        """

        :param value: Value for this filter
        :return: Filter with new configuration appended.
        """
        ftr = {'type': 'size', 'val': value}
        self.filters.append(ftr)

        return self

    def ftr_and(self):
        """
        Sets the next comparison between the last and next filter to be appended using Bitwise AND.
        :return: Filter with new comparison appended.
        """
        self.ftr = FilterType.And

        return self

    def ftr_or(self):
        """
        Sets the next comparison between the last and next filter to be appended using Bitwise OR.
        :return: Filter with new comparison appended.
        """
        self.ftr = FilterType.Or

        return self

    def ftr_not(self):
        """
        Sets the next comparison between the last and next filter to be appended using Bitwise NOT.
        :return: Filter with new comparison appended.
        """
        self.ftr = FilterType.Not

        return self
