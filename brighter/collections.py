from typing import Generator, List


def batch(items: List, n=1) -> Generator:
    """Itera una lista de elementos armando batches de longitud `n`.
    Por ejemplo:
    ```python
    lista = [1, 2, 3, 4, 5]
    for minibatch in batch(lista, n=2):
        print(minibatch)
    ```
    Va a mostrar:
    ```
    >> [1, 2]
    >> [3, 4]
    >> [5]
    ```

    Args:
        items (List): Lista a iterar.
        n (int, optional): Tamaño del batch. Defaults to 1.

    Yields:
        Generator: Generador de batches.
    """
    l = len(items)
    for ndx in range(0, l, n):
        yield items[ndx : min(ndx + n, l)]
