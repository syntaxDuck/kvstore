from pydantic import BaseModel


class NodeDetails(BaseModel):
    id: int
    role: str
    host: str
    port: int

    @property
    def address(self):
        return (self.host, self.port)
