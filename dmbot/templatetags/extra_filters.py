from django import template
register = template.Library()

@register.filter
def sum_values(values):
    try:
        return sum(values)
    except:
        return 0

@register.filter
def map_attribute(queryset, attribute):
    return [getattr(obj, attribute, 0) for obj in queryset]