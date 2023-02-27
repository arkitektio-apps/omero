import sys
from typing import List, Optional
import numpy as np
import xarray as xr
from concurrent.futures import ThreadPoolExecutor
import asyncio
from arkitekt import easy
from fakts.grants.remote.device_code import DeviceCodeGrant
from fakts.grants.remote.base import StaticDiscovery
from fakts import Fakts
from mikro.api.schema import (
    RepresentationFragment,
    from_xarray,
    RepresentationVariety,
    OmeroFileFragment,
    ExperimentFragment,
    SampleFragment,
    OmeroFileType,
    create_experiment,
    create_sample,
    OmeroRepresentationInput,
    ObjectiveSettingsInput,
    ImagingEnvironmentInput,
    PlaneInput,
    PhysicalSizeInput,
    ChannelInput,
    create_instrument,
)
from rekuest.actors.functional import (
    CompletlyThreadedActor,
)
from pydantic import Field
from aicsimageio import AICSImage
from aicsimageio.metadata.utils import bioformats_ome
from ome_types import from_xml
from ome_types.model import Pixels
import logging
import tifffile


logger = logging.getLogger(__name__)

app = easy("converter", url="http://herre:8000/f/")


@app.rekuest.register()
def convert_omero_file(
    file: OmeroFileFragment,
    experiment: Optional[ExperimentFragment],
    sample: Optional[SampleFragment],
    auto_create_sample: bool = True,
) -> List[RepresentationFragment]:
    """Convert Omero File

    Converts an a Mikro File in a Usable zarr based Image

    Args:
        file (OmeroFileFragment): The File to be converted
        sample (Optional[SampleFragment], optional): The Sample to which the Image belongs. Defaults to None.
        experiment (Optional[ExperimentFragment], optional): The Experiment to which the Image belongs. Defaults to None.
        auto_create_sample (bool, optional): Should we automatically create a sample if none is provided?. Defaults to True.

    Returns:
        List[RepresentationFragment]: The created series in this file
    """

    images = []

    if not sample and auto_create_sample:
        sample = create_sample(
            name=file.name, experiments=[experiment] if experiment else []
        )

        print(sample)

    assert file.file, "No File Provided"
    with file.file as f:

        x = bioformats_ome(f)
        print(x)
        img = AICSImage(f)

        for scene in img.scenes:

            print(img.metadata)
            img.set_scene(scene)
            print(img.xarray_dask_data)

        print(img.scenes)

    raise Exception

    return images

@register()
def convert_omero_file_aicos(
    file: OmeroFileFragment,
    stage: Optional[StageFragment],
) -> List[RepresentationFragment]:
    """Convert Omero AIsdfsdfCS

    Converts an a Mikro File in a Usable zarr based Image

    Args:
        file (OmeroFileFragment): The File to be converted
        sample (Optional[SampleFragment], optional): The Sample to which the Image belongs. Defaults to None.
        experiment (Optional[ExperimentFragment], optional): The Experiment to which the Image belongs. Defaults to None.
        auto_create_sample (bool, optional): Should we automatically create a sample if none is provided?. Defaults to True.

    Returns:
        List[RepresentationFragment]: The created series in this file
    """

    images = []
    assert file.file, "No File Provided"
    with file.file as f:
        img = AICSImage(f, reader=BioFile)

        meta = img.ome_metadata

        instrument_map = {}

        for instrument in meta.instruments:
            instrument_map[instrument.id] = create_instrument(
                name=instrument.id,
                # dichroics=instrument.dichroics,
                # filters=instrument.filters,
                lot_number=instrument.microscope.lot_number
                if instrument.microscope
                else None,
                serial_number=instrument.microscope.serial_number
                if instrument.microscope
                else None,
                manufacturer=instrument.microscope.manufacturer
                if instrument.microscope
                else None,
            )

        for index, scene in enumerate(img.scenes):
            img.set_scene(scene)
            array = img.xarray_dask_data
            image = meta.images[index]
            pixels = meta.images[index].pixels

            position = None

            if stage and len(pixels.planes) > 0:
                first_plane = pixels.planes[0]
                position = create_position(
                    stage,
                    x=first_plane.position_x,
                    y=first_plane.position_y,
                    z=first_plane.position_z,
                )

            images.append(
                from_xarray(
                    array,
                    name=image.name,
                    file_origins=[file],
                    tags=["converted"],
                    omero=OmeroRepresentationInput(
                        planes=[
                            PlaneInput(
                                z=p.the_z,
                                c=p.the_c,
                                t=p.the_t,
                                exposureTime=p.exposure_time,
                                deltaT=p.delta_t,
                                positionX=p.position_x,
                                positionY=p.position_y,
                                positionZ=p.position_z,
                            )
                            for p in pixels.planes
                        ],
                        position=position,
                        acquisitionDate=image.acquisition_date,
                        physicalSize=PhysicalSizeInput(
                            x=pixels.physical_size_x,
                            y=pixels.physical_size_y,
                            z=pixels.physical_size_z,
                        ),
                        instrument=instrument_map.get(image.instrument_ref.id, None)
                        if image.instrument_ref
                        else None,
                        channels=[
                            ChannelInput(
                                name=c.name,
                                emmissionWavelength=c.emission_wavelength,
                                excitationWavelength=c.excitation_wavelength,
                                acquisitionMode=c.acquisition_mode.value
                                if c.acquisition_mode
                                else None,
                                color=c.color.as_rgb(),
                            )
                            for c in pixels.channels
                        ],
                        objectiveSettings=ObjectiveSettingsInput(
                            correctionCollar=image.objective_settings.correction_collar,
                            medium=str(image.objective_settings.medium.value).upper()
                            if image.objective_settings.medium
                            else None,
                        )
                        if image.objective_settings
                        else None,
                        imagingEnvironment=ImagingEnvironmentInput(
                            airPressure=image.imaging_environment.air_pressure,
                            co2Percent=image.imaging_environment.co2_percent,
                            humidity=image.imaging_environment.humidity,
                            temperature=image.imaging_environment.temperature,
                        )
                        if image.imaging_environment
                        else None,
                    ),
                )
            )

    return images

with app:
    app.rekuest.run()

