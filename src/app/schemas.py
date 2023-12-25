from pydantic import BaseModel


class Address(BaseModel):
    address: str
    port: int

    def __str__(self):
        return f'{self.address}:{self.port}'
