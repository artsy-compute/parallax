import type { Color, PaletteColorOptions, PaletteMode, PaletteOptions } from '@mui/material';

declare module '@mui/material' {
  interface Color {
    150: string;
    250: string;
    1000: string;
  }
  interface PaletteColor {
    darker: string;
    lighter: string;
    chip: string;
    chipText: string;
  }
  interface SimplePaletteColorOptions {
    darker: string;
    lighter: string;
    chip: string;
    chipText: string;
  }

  interface Palette {
    brand: PaletteColor;
  }
  interface PaletteOptions {
    brand?: PaletteColorOptions;
  }

  interface TypeBackground {
    area: string;
  }
}

declare module '@mui/material/Chip' {
  interface ChipPropsColorOverrides {
    brand: true;
  }
}

const white = '#ffffff';
const black = '#000000';

const brand: Partial<Color> = {
  500: '#05aa6c',
  100: '#cdeee4',
  50: '#dff0e7',
};
const lightGrey: Partial<Color> = {
  50: '#FAFAFA',
  100: '#F7F7F7',
  200: '#F1F1F1',
  250: '#e4e4e4',
  300: '#D6D6D6',
  400: '#BBBBBB',
  500: '#A0A0A0',
  600: '#858585',
  700: '#6A6969',
  800: '#4F4E4E',
  900: '#343333',
  1000: '#191818',
};
const darkGrey: Partial<Color> = {
  50: '#111111',
  100: '#171717',
  200: '#202020',
  250: '#2b2a2a',
  300: '#3a3a3a',
  400: '#525252',
  500: '#6e6e6e',
  600: '#909090',
  700: '#b8b8b8',
  800: '#d9d9d9',
  900: '#f1f1f1',
  1000: '#fafafa',
};
const red: Partial<Color> = {
  100: '#f5e5dd',
  200: '#efbcb3',
  400: '#f06a64',
  500: '#d92d20',
};
const orange: Partial<Color> = {
  100: '#f5e9cf',
  200: '#e99f55',
  500: '#ec7804',
};
const green: Partial<Color> = {
  100: '#e0f0e2',
  200: '#b0dbc3',
  500: '#079455',
};

export const getPalette = (mode: PaletteMode): PaletteOptions => {
  if (mode === 'dark') {
    const grey = darkGrey;
    return {
      mode,
      common: { white, black },
      grey,
      primary: {
        main: grey[100]!,
        darker: grey[50]!,
        dark: grey[50]!,
        light: grey[200]!,
        lighter: grey[300]!,
        contrastText: grey[1000]!,
        chip: grey[900]!,
        chipText: grey[100]!,
      },
      secondary: {
        main: grey[900]!,
        darker: grey[1000]!,
        dark: grey[1000]!,
        light: grey[800]!,
        lighter: grey[700]!,
        contrastText: grey[100]!,
        chip: '#163a2d',
        chipText: '#7ae0b4',
      },
      brand: {
        main: '#1cc989',
        darker: brand[500]!,
        dark: brand[500]!,
        light: '#31d698',
        lighter: '#163a2d',
        contrastText: grey[1000]!,
        chip: '#163a2d',
        chipText: '#7ae0b4',
      },
      info: {
        main: grey[800]!,
        dark: grey[700]!,
        darker: grey[900]!,
        light: grey[600]!,
        lighter: grey[800]!,
        contrastText: grey[100]!,
        chip: grey[800]!,
        chipText: grey[100]!,
      },
      error: {
        main: '#ef6b64',
        dark: red[500]!,
        darker: red[500]!,
        light: '#f39a95',
        lighter: '#3f1f1d',
        contrastText: grey[100]!,
        chip: '#3f1f1d',
        chipText: '#f7b3ae',
      },
      warning: {
        main: '#f0a43b',
        dark: orange[500]!,
        darker: orange[500]!,
        light: '#f6c57f',
        lighter: '#3c2a14',
        contrastText: grey[1000]!,
        chip: '#3c2a14',
        chipText: '#f8cf8a',
      },
      success: {
        main: '#3bc57c',
        dark: green[500]!,
        darker: green[500]!,
        light: '#76d7a0',
        lighter: '#143726',
        contrastText: grey[1000]!,
        chip: '#143726',
        chipText: '#8ad3a8',
      },
      text: {
        primary: grey[900]!,
        secondary: grey[700]!,
        disabled: grey[500]!,
      },
      divider: grey[250]!,
      background: {
        default: grey[50]!,
        paper: grey[100]!,
        area: grey[200]!,
      },
    };
  }

  const grey = lightGrey;
  return {
    mode,
    common: { white, black },
    grey,
    primary: {
      main: grey[900]!,
      darker: grey[1000]!,
      dark: grey[1000]!,
      light: grey[800]!,
      lighter: grey[700]!,
      contrastText: grey[100]!,
      chip: grey[100]!,
      chipText: grey[800]!,
    },
    secondary: {
      main: grey[100]!,
      darker: grey[250]!,
      dark: grey[200]!,
      light: grey[200]!,
      lighter: grey[300]!,
      contrastText: grey[900]!,
      chip: brand[100]!,
      chipText: brand[500]!,
    },
    brand: {
      main: brand[500]!,
      darker: brand[500]!,
      dark: brand[500]!,
      light: brand[500]!,
      lighter: brand[50]!,
      contrastText: grey[100]!,
      chip: brand[100]!,
      chipText: brand[500]!,
    },
    info: {
      main: white,
      dark: grey[500]!,
      darker: grey[250]!,
      light: grey[100]!,
      lighter: grey[100]!,
      contrastText: grey[800]!,
      chip: grey[200]!,
      chipText: grey[800]!,
    },
    error: {
      main: red[500]!,
      dark: red[500]!,
      darker: red[500]!,
      light: red[400]!,
      lighter: red[100]!,
      contrastText: grey[100]!,
      chip: red[100]!,
      chipText: red[500]!,
    },
    warning: {
      main: orange[500]!,
      dark: orange[500]!,
      darker: orange[500]!,
      light: orange[500]!,
      lighter: orange[100]!,
      contrastText: grey[100]!,
      chip: orange[100]!,
      chipText: orange[500]!,
    },
    success: {
      main: green[500]!,
      dark: green[500]!,
      darker: green[500]!,
      light: green[500]!,
      lighter: green[100]!,
      contrastText: grey[100]!,
      chip: red[100]!,
      chipText: brand[500]!,
    },
    text: {
      primary: grey[900]!,
      secondary: grey[700]!,
      disabled: grey[500]!,
    },
    divider: grey[250]!,
    background: {
      default: white,
      paper: white,
      area: grey[200]!,
    },
  };
};

export const palette: PaletteOptions = getPalette('light');
