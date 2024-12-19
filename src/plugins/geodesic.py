from branca.element import MacroElement

from folium.elements import JSCSSMixin
from folium.template import Template


class Geodesic(JSCSSMixin, MacroElement):
    '''
    Examples
    --------
    >>> m = folium.Map()
    >>> Draw(
    ...     export=True,
    ...     filename="my_data.geojson",
    ...     show_geometry_on_click=False,
    ...     position="topleft",
    ...     draw_options={"polyline": {"allowIntersection": False}},
    ...     edit_options={"poly": {"allowIntersection": False}},
    ...     on={
    ...         "click": JsCode(
    ...             """
    ...         function(event) {
    ...            alert(JSON.stringify(this.toGeoJSON()));
    ...         }
    ...     """
    ...         )
    ...     },
    ... ).add_to(m)

    For more info please check
    https://leaflet.github.io/Leaflet.draw/docs/leaflet-draw-latest.html

    '''

    _template = Template(
        """
        {% macro script(this, kwargs) %}
            const Berlin = {lat: 52.5, lng: 13.35};
            const LosAngeles = {lat: 33.82, lng: -118.38};
            const geodesic = new L.Geodesic([Berlin, LosAngeles]).addTo({{ this._parent.get_name() }});
        {% endmacro %}
        """
    )

    default_js = [
        (
            "leaflet.geodesic",
            "https://cdn.jsdelivr.net/npm/leaflet.geodesic",
        )
    ]

    def __init__(
        self,
    ):
        super().__init__()
        self._name = "DrawControl"

