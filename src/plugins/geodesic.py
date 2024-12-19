from branca.element import MacroElement

from folium.elements import JSCSSMixin
from folium.template import Template


class Geodesic(JSCSSMixin, MacroElement):
    _template = Template(
        """
        {% macro script(this, kwargs) %}
            var geodesic = new L.Geodesic([
                {lat: {{ this.latitude_a }}, lng: {{ this.longitude_a }} },
                {lat: {{ this.latitude_b }}, lng: {{ this.longitude_b }} }
            ]).addTo({{ this._parent.get_name() }});
        {% endmacro %}
        """
    )

    default_js = [
        (
            "leaflet.geodesic",
            "https://cdn.jsdelivr.net/npm/leaflet.geodesic",
        )
    ]

    def __init__(self, latitude_a, longitude_a, latitude_b, longitude_b):
        super().__init__()
        self._name = "DrawControl"
        self.longitude_b = longitude_b
        self.latitude_a = latitude_a
        self.longitude_a = longitude_a
        self.latitude_b = latitude_b
