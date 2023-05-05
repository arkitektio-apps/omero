import sys
from typing import List, Optional
import numpy as np
import xarray as xr
from concurrent.futures import ThreadPoolExecutor
import asyncio
from arkitekt import register, group
from mikro.api.schema import (
    RepresentationFragment,
    from_xarray,
    OmeroFileFragment,
    DatasetFragment,
    OmeroRepresentationInput,
    ObjectiveSettingsInput,
    ImagingEnvironmentInput,
    PlaneInput,
    PhysicalSizeInput,
    ChannelInput,
    StageFragment,
    RepresentationViewInput,
    create_instrument,
    create_position,
    create_timepoint,

    create_channel,
    Dimension,
    EraFragment,
    
)
from ome_types.model import Pixels
import logging
import tifffile
from aicsimageio.metadata.utils import bioformats_ome
from scyjava import config, jimport
import scyjava

logger = logging.getLogger(__name__)
x = config

def load_as_xarray(path: str, series: str, pixels: Pixels):
    if path.endswith((".stk", ".tif", ".tiff", ".TIF")):
        image = tifffile.imread(path)
        print(image.shape)

        image = image.reshape((1,) * (5 - image.ndim) + image.shape)
        return xr.DataArray(image, dims=list("ctzyx"))

    else:
        raise NotImplementedError("Only tiff supported at the moment. Because of horrendous python bioformats performance and memory leaks.")


@register(port_groups=[group(key="advanced")], groups={"position_from_planes": ["advanced"], "channels_from_channels": ["advanced"], "position_tolerance": ["advanced"], "timepoint_from_time": ["advanced"], "timepoint_tolerance": ["advanced"]})
def convert_omero_file(
    file: OmeroFileFragment,
    stage: Optional[StageFragment],
    era: Optional[EraFragment],
    dataset: Optional[DatasetFragment],
    position_from_planes: bool = True,
    timepoint_from_time: bool = True,
    channels_from_channels: bool = True,
    position_tolerance: Optional[float] = None,
    timepoint_tolerance: Optional[float] = None,
) -> List[RepresentationFragment]:
    """Convert Omero

    Converts an Omero File in a set of Mikrodata

    Args:
        file (OmeroFileFragment): The File to be converted
        stage (Optional[StageFragment], optional): The Stage in which to put the Image. Defaults to None.
        era (Optional[EraFragment], optional): The Era in which to put the Image.. Defaults to None.
        dataset (Optional[DatasetFragment], optional): The Dataset in which to put the Image. Defaults to the file dataset.
        position_from_planes (bool, optional): Whether to create a position from the first planes (only if stage is provided). Defaults to True.
        timepoint_from_time (bool, optional): Whether to create a timepoint from the first time (only if era is provided). Defaults to True.
        channels_from_channels (bool, optional): Whether to create a channel from the channels. Defaults to True.
        position_tolerance (Optional[float], optional): The tolerance for the position. Defaults to no tolerance.
        timepoint_tolerance (Optional[float], optional): The tolerance for the timepoint. Defaults  to no tolerance.

    Returns:
        List[RepresentationFragment]: The created series in this file
    """

    images = []

    assert file.file, "No File Provided"
    with file.file as f:
        meta = bioformats_ome(f)
        print(meta)
        instrument_map = {}

        for instrument in meta.instruments:
            if instrument.id:
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

        for index, image in enumerate(meta.images):
            # we will create an image for every series here
            pixels = image.pixels
            print(pixels)


            views = []
            # read array (at the moment fake)
            array = load_as_xarray(f, index, pixels=pixels)

            position = None
            timepoint = None

            if stage and position_from_planes and len(pixels.planes) > 0:
                first_plane = pixels.planes[0]
                position = create_position(
                    stage,
                    x=first_plane.position_x or 0,
                    y=first_plane.position_y or 0,
                    z=1,
                    tolerance=position_tolerance,
                )

            if era and timepoint_from_time and image.acquisition_date:
                assert era.start, "Era needs to have a start"
                first_plane = pixels.planes[0]
                timepoint = create_timepoint(
                    era,
                    delta_t=(image.acquisition_date - era.start.replace(tzinfo=None)).microseconds,
                    tolerance=timepoint_tolerance,
                )
                print(timepoint)




            if channels_from_channels:
                for index, c in enumerate(pixels.channels):
                    c = create_channel(
                                    name=c.name or f"Channel {index}",
                                    emission_wavelength=c.emission_wavelength,
                                    excitation_wavelength=c.excitation_wavelength,
                                    acquisition_mode=c.acquisition_mode.value
                                    if c.acquisition_mode
                                    else None,
                                    color=c.color.as_rgb() if c.color else None,
                                )
                    
                    views.append(RepresentationViewInput(
                        cMin=index,
                        cMax=index,
                        channel=c
                    ))
                          

            rep = from_xarray(
                    array,
                    name=file.name + " - "  + (image.name if image.name else f"({index})"),
                    datasets=[dataset] if dataset else file.datasets,
                    file_origins=[file],
                    tags=["converted"],
                    views=views,
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
                        timepoints=[timepoint] if timepoint else None,
                        positions=[position] if position else None,
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
            
              



            images.append(
                rep
            )

    return images


@register()
def convert_tiff_file(
    file: OmeroFileFragment,
    stage: Optional[StageFragment],
    dataset: Optional[DatasetFragment],
) -> List[RepresentationFragment]:
    """Convert Tiff File

    Converts an tilffe File in a set of Mikrodata

    Args:
        file (OmeroFileFragment): The File to be converted
        sample (Optional[SampleFragment], optional): The Sample to which the Image belongs. Defaults to None.
        experiment (Optional[ExperimentFragment], optional): The Experiment to which the Image belongs. Defaults to None.
        auto_create_sample (bool, optional): Should we automatically create a sample if none is provided?. Defaults to True.
        position_from_planes (bool, optional): Should we use the first planes position to put the image into context

    Returns:
        List[RepresentationFragment]: The created series in this file
    """

    images = []

    assert file.file, "No File Provided"
    with file.file as f:
        image = tifffile.imread(f)

        image = image.reshape((1,) * (5 - image.ndim) + image.shape)
        array = xr.DataArray(image, dims=list("ctzyx"))

        images.append(
            from_xarray(
                array,
                name=file.name,
                datasets=[dataset] if dataset else file.datasets,
                file_origins=[file],
                tags=["converted"],
            )
        )

    return images