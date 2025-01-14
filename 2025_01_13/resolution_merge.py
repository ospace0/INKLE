from netCDF4 import Dataset
import os

def extract_image_pixel_values(input_file, output_file):
    """
    Extracts the 'image_pixel_values' variable from a NetCDF file
    and saves it to a new NetCDF file.

    Parameters:
        input_file (str): Path to the input NetCDF file.
        output_file (str): Path to save the new NetCDF file.
    """
    try:
        with Dataset(input_file, 'r') as src:
            if 'image_pixel_values' not in src.variables:
                print("Variable 'image_pixel_values' not found in the input file.")
                return

            with Dataset(output_file, 'w') as dst:
                # Copy dimensions
                for name, dim in src.dimensions.items():
                    dst.createDimension(name, len(dim) if not dim.isunlimited() else None)

                # Copy the 'image_pixel_values' variable
                var = src.variables['image_pixel_values']
                out_var = dst.createVariable('image_pixel_values', var.datatype, var.dimensions)
                out_var.setncatts({attr: var.getncattr(attr) for attr in var.ncattrs()})
                out_var[:] = var[:]

                print(f"Extracted 'image_pixel_values' to {output_file}")

    except Exception as e:
        print(f"Error during extraction: {e}")

def merge_nc_files_by_resolution(data_types_list, input_directory, output_directory):
    """
    Merges NetCDF files by resolution into separate files.

    Parameters:
        data_types_list (dict): A dictionary mapping resolutions to data type lists.
        input_directory (str): Directory containing input NetCDF files.
        output_directory (str): Directory to save merged NetCDF files.
    """
    try:
        for resolution, data_types in data_types_list.items():
            output_file = os.path.join(output_directory, f"Merged_{resolution.replace('.', '_')}.nc")

            with Dataset(output_file, 'w') as dst:
                first_file = None

                for data_type in data_types:
                    # Match file names like IR087_202501080000.nc
                    input_file = next((
                        os.path.join(input_directory, f) 
                        for f in os.listdir(input_directory) 
                        if f.startswith(data_type) and f.endswith(".nc")
                    ), None)

                    if not input_file or not os.path.exists(input_file):
                        print(f"File not found for data type: {data_type}")
                        continue

                    with Dataset(input_file, 'r') as src:
                        if first_file is None:
                            # Copy dimensions from the first file
                            for name, dim in src.dimensions.items():
                                dst.createDimension(name, len(dim) if not dim.isunlimited() else None)
                            first_file = input_file

                        # Add the variable for this data type
                        if 'image_pixel_values' in src.variables:
                            var = src.variables['image_pixel_values']
                            out_var = dst.createVariable(data_type, var.datatype, var.dimensions)
                            out_var.setncatts({attr: var.getncattr(attr) for attr in var.ncattrs()})
                            out_var[:] = var[:]
                            print(f"Added data type '{data_type}' to {output_file}")

            print(f"Merged file saved for resolution {resolution} at {output_file}")

    except Exception as e:
        print(f"Error during merging: {e}")

# Data types mapping by resolution
data_types_list = {
    "2": ["NR013", "NR016", "SW038", "WV063", "WV069", "IR087", "IR096", "IR105", "IR112", "IR123", "IR133", "WV073"],
    "1": ["VI004", "VI005", "VI008"],
    "0.5": ["VI006"]
}

# Directories
input_directory = "F:/INKLE/2025_01_13/RAW"
output_directory = "F:/INKLE/2025_01_13"

# Merge files by resolution
merge_nc_files_by_resolution(data_types_list, input_directory, output_directory)
