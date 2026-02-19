import requests
import zipfile
import pandas as pd
from pathlib import Path
from io import BytesIO


def download_file(url: str) -> bytes:
    """Télécharge le fichier et retourne son contenu en mémoire"""
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        content = response.content
    return content


def extract_zip(zip_filename: str, extract_to: str = ".") -> None:
    """
    Extrait le contenu d'une archive ZIP dans un répertoire cible.

    Parameters
    ----------
    zip_filename : str
        Chemin vers le fichier ZIP à extraire.
    extract_to : str, optional
        Répertoire de destination des fichiers extraits
        (par défaut : répertoire courant).

    Raises
    ------
    FileNotFoundError
        Si le fichier ZIP n'existe pas.
    zipfile.BadZipFile
        Si le fichier fourni n'est pas une archive ZIP valide.
    """

    with zipfile.ZipFile(zip_filename, "r") as z:
        z.extractall(extract_to)
    print(f"Extraction terminée dans le dossier : {extract_to}")


def float_to_codepostal(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Convertit une colonne contenant des codes postaux numériques en format chaîne à 5 caractères.

    Cette fonction est destinée aux cas où les codes postaux ont été lus comme
    des nombres flottants (ex. `1400.0`) et doivent être restaurés en chaînes
    avec zéros initiaux (ex. `01400`).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant la colonne à transformer.
    col : str
        Nom de la colonne contenant les codes postaux.

    Returns
    -------
    pandas.DataFrame
        DataFrame avec la colonne des codes postaux convertie en chaînes
        de longueur 5.

    Notes
    -----
    - La fonction modifie le DataFrame en place et le retourne.
    - Les valeurs manquantes sont converties en chaînes `'nan'`
      si elles ne sont pas nettoyées en amont.
    """

    df[col] = df[col].astype(str).str.replace(".0", "", regex=False).str.zfill(5)
    return df


def homogene_nan(df):
    cols = ["adrs_codeinsee", "adrs_codepostal"]
    invalid_values = ["nan", "<NA>", "NaN", "Nan", "0", "0.0", "", "INSEE", "commune"]
    for col in cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(invalid_values, pd.NA)
        df = float_to_codepostal(df, col)
    return df


def create_dataframe_communes() -> pd.DataFrame:
    """
    Crée un DataFrame des communes à partir d'une source en ligne.
    """
    com_url = (
        "https://www.data.gouv.fr/api/1/datasets/r/f5df602b-3800-44d7-b2df-fa40a0350325"
    )
    content = download_file(com_url)
    df_com = pd.read_csv(BytesIO(content), sep=",", low_memory=False)
    df_com = float_to_codepostal(df_com, "code_postal")
    return df_com


def get_raw_dir() -> Path:
    """Retourne le chemin du répertoire source, le crée si nécessaire."""
    base_dir = Path(__file__).resolve().parent.parent
    raw_dir = base_dir / "source"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir
