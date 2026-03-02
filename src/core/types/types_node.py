from pydantic import BaseModel


class NodeDetails(BaseModel):
    id: int
    role: str
    host: str
    port: int

    @property
    def address(self):
        return (self.host, self.port)

    def __str__(self) -> str:
        return f"Node: {self.id}, Role: {self.role}, Addr: {self.address}"
