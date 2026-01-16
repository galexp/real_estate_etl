import pandas as pd

class PropertyTransformer:
    """
    Attaches agent_id to properties
    """

    def attach_agent_id(
        self,
        properties_df: pd.DataFrame,
        agents_df: pd.DataFrame
    ) -> pd.DataFrame:

        df = properties_df.merge(
            agents_df,
            left_on="agentName",
            right_on="agent_name",
            how="left"
        )

        return df
