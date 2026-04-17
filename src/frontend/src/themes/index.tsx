'use client';

import type { FC, PropsWithChildren } from 'react';
import { createContext, useContext, useEffect, useMemo, useState } from 'react';

import type { PaletteMode, Theme } from '@mui/material';
import { createTheme, THEME_ID, ThemeProvider as MuiThemeProvider } from '@mui/material';

import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';

import { getPalette } from './palette';
import { HTML_FONT_SIZE, typography } from './typography';
import { overlays } from './shadows';
import * as themeComponents from './components';
import { SnackbarProvider } from './components';

const THEME_MODE_STORAGE_KEY = 'parallax.theme-mode';

const ThemeModeContext = createContext<{
  mode: PaletteMode;
  setMode: (mode: PaletteMode) => void;
  toggleMode: () => void;
} | null>(null);

const buildTheme = (mode: PaletteMode) => {
  const materialTheme = createTheme({
    palette: getPalette(mode),
    typography,
    spacing: (factor: number) => `${(factor * 8) / HTML_FONT_SIZE}rem`,
    overlays,
  });

  materialTheme.components = materialTheme.components || {};

  (
    Object.entries(themeComponents) as [
      keyof NonNullable<Theme['components']>,
      (theme: Theme) => NonNullable<Theme['components']>[keyof NonNullable<Theme['components']>],
    ][]
  ).forEach(
    <K extends keyof NonNullable<Theme['components']>>([compName, generate]: [
      K,
      (theme: Theme) => NonNullable<Theme['components']>[K],
    ]) => {
      if (compName.startsWith('Mui') && typeof generate === 'function') {
        materialTheme.components![compName] = generate(materialTheme);
      }
    },
  );

  return materialTheme;
};

const Provider: FC<PropsWithChildren> = ({ children }) => {
  const [mode, setMode] = useState<PaletteMode>(() => {
    if (typeof window === 'undefined') {
      return 'light';
    }
    const stored = window.localStorage.getItem(THEME_MODE_STORAGE_KEY);
    return stored === 'dark' ? 'dark' : 'light';
  });

  useEffect(() => {
    if (typeof window !== 'undefined') {
      window.localStorage.setItem(THEME_MODE_STORAGE_KEY, mode);
    }
  }, [mode]);

  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.style.colorScheme = mode;
    }
  }, [mode]);

  const theme = useMemo(() => buildTheme(mode), [mode]);
  const contextValue = useMemo(
    () => ({
      mode,
      setMode,
      toggleMode: () => setMode((prev) => (prev === 'light' ? 'dark' : 'light')),
    }),
    [mode],
  );

  return (
    <ThemeModeContext.Provider value={contextValue}>
      <MuiThemeProvider theme={{ [THEME_ID]: theme }}>
        <SnackbarProvider>
          <LocalizationProvider dateAdapter={AdapterDayjs} localeText={{ okButtonLabel: 'Apply' }}>
            {children}
          </LocalizationProvider>
        </SnackbarProvider>
      </MuiThemeProvider>
    </ThemeModeContext.Provider>
  );
};

export { Provider as ThemeProvider };

export const useThemeMode = () => {
  const context = useContext(ThemeModeContext);
  if (context == null) {
    throw new Error('useThemeMode must be used within ThemeProvider');
  }
  return context;
};
