import warnings

import pandas as pd
import plotly.graph_objects as go

from camels_de1h import get_output_path, get_nuts_mapping


class Station1h():
    """
    Class for handling station data and metadata for CAMELS-DE in hourly resolution.

    """
    def __init__(self, gauge_id: str):
        """
        Parameters
        ----------
        gauge_id : str
            The station id of the station to be handled (nuts_id). Can also be the provider id.

        """
        # get the mapping
        mapping = get_nuts_mapping(format="csv")

        # make sure that gauge_id is a string
        gauge_id = str(gauge_id)

        # check if gauge_id is a nuts_id or a provider_id
        if gauge_id in mapping.provider_id.values:
            provider_id = gauge_id
            self.gauge_id = mapping.set_index("provider_id").loc[provider_id, "nuts_id"]
        elif gauge_id in mapping.nuts_id.values:
            self.gauge_id = gauge_id
        else:
            raise ValueError(f"{gauge_id} is neither a provider_id nor a CAMELS-DE NUTSID. If it is a provider_id that you want to include in the dataset, please add it to the mapping first with get_nuts_id_from_provider_id(add_missing=True).")

        # set the output path
        self.output_path = get_output_path(self.gauge_id)

        # set the data path if data was already generated, else set to None
        data_file = self.output_path / f"{self.gauge_id}_data.csv"
        if data_file.exists():
            self.data_path = data_file
        else:
            self.data_path = None

        # TODO: replace all below where camelsp is used with infomation from self.metadata
        
        # go for metadata fields if available
        if self.has_metadata:
            # metadata
            meta = self.get_metadata()
            self.metadata = meta

            # name
            self.name = self.metadata.gauge_name.values[0]

            # provider id
            self.provider_id = self.metadata.provider_id.values[0]

            # water body name
            self.water_body_name = self.metadata.water_body_name.values[0]

            # federal state
            self.bl = self.metadata.federal_state.values[0]

            # location
            self.lat = self.metadata.lat.values[0]
            self.lon = self.metadata.lon.values[0]
            self.northing = self.metadata.northing.values[0]
            self.easting = self.metadata.easting.values[0]

            # elevation
            self.elevation = self.metadata.elev_metadata.values[0]

            # area
            self.area = self.metadata.area_metadata.values[0]
        else:
            # set everything to None
            self.metadata = None
            self.name = None
            self.provider_id = None
            self.water_body_name = None
            self.bl = None
            self.lat = None
            self.lon = None
            self.northing = None
            self.easting = None

            self.elevation = None
            self.area = None

    def __repr__(self):
        return f"Station1h({self.gauge_id})"

    @property
    def has_metadata(self):
        """
        Check if metadata is available / already saved for the station.

        """
        meta_file = self.output_path / f"{self.gauge_id}_metadata.csv"
        return meta_file.exists()

    def save_data(self, data: pd.DataFrame):
        """
        Save the hydrologic data for the station. The data should be a pandas dataframe
        with a datetime index or a date column and another column 'discharge_vol_obs' or 
        'water_level_obs'. If one of the columns is missing, it will be added as NaN.  
        * discharge_vol_obs: observed discharge in [m³/s]
        * water_level_obs: observed water level in [cm]  

        All data must be in hourly resolution with no gaps in the date range.

        The function also checks the data for the following:
        * duplicated dates
        * sorted dates
        * missing dates
        * additional columns
                
        Note that date has to be in UTC+0.

        """
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas dataframe.")

        # check if data has the required columns, if not, add them as NaN
        if "discharge_vol_obs" not in data.columns:
            data["discharge_vol_obs"] = None 
        if "water_level_obs" not in data.columns:
            data["water_level_obs"] = None
        
        # sort columns
        data = data[["date", "discharge_vol_obs", "water_level_obs"]]
        
        if "date" in data.columns:
            data = data.set_index("date")

        # check for duplicated dates
        if data.index.duplicated().any():
            raise ValueError(f"{self.gauge_id}: Data contains duplicated dates.")
        
        # check if dates are sorted
        if not data.index.is_monotonic_increasing:
            raise ValueError(f"{self.gauge_id}: Data is not sorted by date.")
        
        # check if the date increases by one hour each timestep
        if not (data.index.to_series().diff().iloc[1:] == pd.Timedelta("1H")).all():
            raise ValueError(f"{self.gauge_id}: Not all dates are in hourly resolution.")
        
        # check if date is in UTC+0
        if str(data.index.tz) != "UTC":
            raise ValueError(f"{self.gauge_id}: Data is not in UTC+0 or it misses the timezone information.")
        
        # check if more columns than required are present and print the names of the additional columns
        additional_columns = data.columns.difference(["discharge_vol_obs", "water_level_obs"])
        if len(additional_columns) > 0:
            warnings.warn(f"{self.gauge_id}: Additional columns found: {additional_columns}")

        # create Bundesland and Station directories
        self.output_path.mkdir(parents=True, exist_ok=True)

        data.to_csv(self.output_path / f"{self.gauge_id}_data.csv", index=True, index_label="date")        

    def get_data(self, date_index: bool = False) -> pd.DataFrame:
        """
        Read the data from the output folder and return as pandas dataframe.
        Pass the CAMELS-DE nuts_id. 
        If date_index is False, 'date' will be a
        data column and a generic range-index is used.

        """
        if self.data_path is None:
            raise FileNotFoundError(f"No data found for station {self.gauge_id}")

        data = pd.read_csv(self.data_path, parse_dates=True)

        if date_index:
            data = data.set_index("date")

        return data
    
    def save_raw_metadata(self, metadata: pd.DataFrame):
        """
        Save the raw metadata for the station. The metadata should be a pandas dataframe 
        containing all the raw metadata information for the station. Usually, the source
        for this is a header in the data file or information from some additional metadata
        file provided by the federal states.

        """
        if not isinstance(metadata, pd.DataFrame):
            raise ValueError("Metadata must be a pandas dataframe.")
        
        # create Bundesland and Station directories
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        metadata.to_csv(self.output_path / f"{self.gauge_id}_raw_metadata.csv", index=False)

    def save_metadata(self, gauge_id: str, provider_id: str, gauge_name: str, water_body_name: str, 
                      federal_state: str, gauge_lon: float, gauge_lat: float, gauge_easting: float, 
                      gauge_northing: float, gauge_elev_metadata: float, area_metadata: float,
                      part_of_camelsp: bool):
        """
        Save the metadata for the station. The metadata is saved to the output folder of the
        station, as well as to a metadata file for all stations in the metadata folder.
        The metadata should be a pandas dataframe 
        containing all the mandatory metadata information for the station.  
        These fields are mandatory (from CAMELS-DE):
        * gauge_id (i.e. camels_id / nuts_id)
        * provider_id (ID used by the federal state)
        * gauge_name
        * waterbody_name
        * federal_state
        * gauge_lon (EPSG:4326)
        * gauge_lat (EPSG:4326)
        * gauge_easting (EPSG:3035)
        * gauge_northing (EPSG:3035)
        * gauge_elev_metadata (m.a.s.l.)
        * area_metadata (km²)
        * part_of_camelsp (whether the station is part of camelsp preprocessing)

        You can also save metadata with missing mandatory fields by setting these fields to None.

        """
        # create Bundesland and Station directories
        self.output_path.mkdir(parents=True, exist_ok=True)

        # create metadata dataframe
        metadata = pd.DataFrame({
            "gauge_id": [gauge_id],
            "provider_id": [provider_id],
            "gauge_name": [gauge_name],
            "water_body_name": [water_body_name],
            "federal_state": [federal_state],
            "lon": [gauge_lon],
            "lat": [gauge_lat],
            "easting": [gauge_easting],
            "northing": [gauge_northing],
            "elev_metadata": [gauge_elev_metadata],
            "area_metadata": [area_metadata],
            "part_of_camelsp": [part_of_camelsp]
        }
        )

        # save metadata
        metadata.to_csv(self.output_path / f"{self.gauge_id}_metadata.csv", index=False)

        # save metadata to metadata folder
        if not (get_output_path(None) / "metadata" / "metadata1h.csv").exists():
            metadata.to_csv(get_output_path(None) / "metadata" / "metadata1h.csv", mode="w", header=True, index=False)
        else:
            # check if metadata for the station was already saved, if yes, replace the line
            meta = pd.read_csv(get_output_path(None) / "metadata" / "metadata1h.csv")

            # check if gauge_id is already in metadata
            if gauge_id in meta.gauge_id.values:
                # remove the old metadata row for the station
                meta = meta[meta.gauge_id != gauge_id]

                # add the new metadata
                with warnings.catch_warnings():
                    # ignore FutureWarning from pandas when concatenating a potentially empty dataframe
                    warnings.simplefilter("ignore", category=FutureWarning)
                    meta = pd.concat([meta, metadata], ignore_index=True).sort_values("gauge_id")

                meta.to_csv(get_output_path(None) / "metadata" / "metadata1h.csv", mode="w", header=True, index=False)
            else:
                metadata.to_csv(get_output_path(None) / "metadata" / "metadata1h.csv", mode="a", header=False, index=False)

    def get_metadata(self) -> pd.DataFrame:
        """
        Read the metadata from the output folder and return as pandas dataframe.

        """
        # check if metadata is available
        if not self.has_metadata:
            raise FileNotFoundError(f"No metadata found for station {self.gauge_id}")
        
        return pd.read_csv(self.output_path / f"{self.gauge_id}_metadata.csv", dtype={"provider_id": str})
    
    def plot(self, data_type: str = "both") -> go.Figure:
        """
        Create an interactive Plotly plot of discharge and/or water level data.

        Parameters
        ----------
        data_type : str, optional
            Type of data to plot. Options: "q" (discharge), "w" (water level), 
            or "both" (default)

        Returns
        -------
        go.Figure
            Plotly figure object containing the interactive plot

        """
        if data_type.lower() not in ["q", "w", "both"]:
            raise ValueError('data_type must be one of "q", "w", or "both"')

        df = self.get_data(date_index=True)
        
        fig = go.Figure()

        # Add discharge trace
        if data_type.lower() in ["q", "both"]:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["discharge_vol_obs"],
                    name="Discharge",
                    line=dict(color="#1f77b4", width=1.5),
                    yaxis="y",
                    hovertemplate="<b>Date</b>: %{x}<br>" +
                                "<b>Discharge</b>: %{y:.2f} m³/s<br><extra></extra>"
                )
            )

        # Add water level trace
        if data_type.lower() in ["w", "both"]:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df["water_level_obs"],
                    name="Water Level",
                    line=dict(color="#ff7f0e", width=1.5),
                    yaxis="y2" if data_type.lower() == "both" else "y",
                    hovertemplate="<b>Date</b>: %{x}<br>" +
                                "<b>Water Level</b>: %{y:.1f} cm<br><extra></extra>"
                )
            )

        # Update layout
        title = f"Station {self.gauge_id}: "
        title += "Discharge & Water Level" if data_type.lower() == "both" else \
                "Discharge" if data_type.lower() == "q" else "Water Level"
        
        fig.update_layout(
            template="simple_white",
            title=dict(
                text=title,
                x=0.5,
                xanchor='center',
                font=dict(size=16)
            ),
            xaxis=dict(
                title="Date",
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(128,128,128,0.2)',
                zeroline=False,
                title_font=dict(size=14)
            ),
            yaxis=dict(
                title="Discharge [m³/s]" if data_type.lower() in ["q", "both"] else "Water Level [cm]",
                side="left",
                showgrid=True,
                gridwidth=1,
                gridcolor='rgba(128,128,128,0.2)',
                zeroline=False,
                title_font=dict(size=14)
            ),
            hovermode="x unified",
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99,
                bgcolor='rgba(255,255,255,0.8)'
            ),
            paper_bgcolor='white',
            plot_bgcolor='white',
            width=1000,
            height=500,
            margin=dict(l=80, r=80, t=100, b=80)
        )

        # Add secondary y-axis
        if data_type.lower() == "both":
            fig.update_layout(
                yaxis2=dict(
                    title="Water Level [cm]",
                    side="right",
                    overlaying="y",
                    showgrid=False,
                    zeroline=False,
                    title_font=dict(size=14)
                )
            )

        # add a range slider
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(count=7, label="1w", step="day", stepmode="backward"),
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
                type="date"
            )
        )

        # add a camel emoji 🐫
        fig.add_annotation(
            text="🐫",
            xref="paper",
            yref="paper",
            x=1.025,  # position beyond plot area
            y=-0.4,    # at the top
            xanchor="left",
            yanchor="top",
            showarrow=False,
            font=dict(size=24),
            xshift=0,
            yshift=-10
        )


        return fig