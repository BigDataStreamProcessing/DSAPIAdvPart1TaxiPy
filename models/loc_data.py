class LocData:
    def __init__(self, location_id=0, borough="", zone="", service_zone=""):
        self.location_id = location_id
        self.borough = borough
        self.zone = zone
        self.service_zone = service_zone

    def __str__(self):
        return (
            f"LocData(location_id={self.location_id}, "
            f"borough='{self.borough}', zone='{self.zone}', service_zone='{self.service_zone}')"
        )
