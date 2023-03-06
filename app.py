import sys
from typing import List, Optional
import numpy as np
import xarray as xr
from concurrent.futures import ThreadPoolExecutor
import asyncio
from arkitekt import register
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
    create_instrument,
    create_position,
)
import bioformats
import javabridge
from ome_types.model import Pixels
import logging
import tifffile
from aicsimageio.metadata.utils import bioformats_ome

logger = logging.getLogger(__name__)


def load_as_xarray(path: str, series: str, pixels: Pixels):
    if path.endswith((".stk", ".tif", ".tiff", ".TIF")):
        image = tifffile.imread(path)
        print(image.shape)

        image = image.reshape((1,) * (5 - image.ndim) + image.shape)
        return xr.DataArray(image, dims=list("ctzyx"))

    else:
        javabridge.start_vm(run_headless=True, class_path=bioformats.JARS)
        javabridge.attach()

        initial_array = xr.DataArray(
            np.zeros(
                (
                    pixels.size_c,
                    pixels.size_t,
                    pixels.size_z,
                    pixels.size_x,
                    pixels.size_y,
                )
            ),
            dims=list("ctzxy"),
        )

        with bioformats.ImageReader(path, perform_init=True) as reader:
            for c in range(pixels.size_c):
                for z in range(pixels.size_z):
                    for t in range(pixels.size_t):
                        # bioformats appears to swap axes for tif images and read all three channels at a time for RGB
                        im1 = reader.read(
                            c=c,
                            z=z,
                            t=t,
                            series=series,
                            rescale=True,
                            channel_names=None,
                        )
                        if im1.ndim == 3:
                            if im1.shape[2] == 3:
                                # Three channels are red
                                im2 = im1[:, :, c]
                            else:
                                im2 = im1
                        else:
                            im2 = im1
                        if (
                            pixels.size_x == im2.shape[1]
                            and pixels.size_y == im2.shape[0]
                        ) and not pixels.size_x == pixels.size_y:
                            # x and y are swapped
                            logging.warning(
                                "Image might be transposed. Swapping X and Y"
                            )
                            im3 = im2.transpose()
                        else:
                            im3 = im2

                        initial_array[
                            c,
                            t,
                            z,
                            :,
                            :,
                        ] = im3

        return initial_array


@register()
def convert_omero_file(
    file: OmeroFileFragment,
    stage: Optional[StageFragment],
    dataset: Optional[DatasetFragment],
    position_from_planes: bool = True,
    position_tolerance: Optional[float] = None,
) -> List[RepresentationFragment]:
    """Convert Omero

    Converts an Omero File in a set of Mikrodata

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
        meta = bioformats_ome(f)
        print(meta)
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

        for index, image in enumerate(meta.images):
            # we will create an image for every series here
            pixels = image.pixels
            print(pixels)

            # read array (at the moment fake)
            array = load_as_xarray(f, index, pixels=pixels)

            position = None

            if stage and position_from_planes and len(pixels.planes) > 0:
                first_plane = pixels.planes[0]
                position = create_position(
                    stage,
                    x=first_plane.position_x or 0,
                    y=first_plane.position_y or 0,
                    z=1,
                    tolerance=position_tolerance,
                )

            images.append(
                from_xarray(
                    array,
                    name=image.name,
                    datasets=[dataset] if dataset else file.datasets,
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
