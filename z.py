import jpype
import scyjava
from aicsimageio.metadata.utils import bioformats_ome

# cause dependency buildup
meta = bioformats_ome("test.tiff")
print(meta)