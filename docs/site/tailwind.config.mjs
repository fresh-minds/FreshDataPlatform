/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        navy: '#1E3A5F',
        'navy-light': '#2A4A73',
        cyan: '#00D4FF',
        'cyan-dark': '#00A3CC',
        mint: '#4ADE80',
        amber: '#FBBF24',
        surface: '#F7F8FA',
        'code-bg': '#0D1117',
        'text-dark': '#1A1A2E',
        'text-light': '#F7F8FA',
        'border-muted': '#E4D6C4',
      },
      fontFamily: {
        heading: ['Inter', 'system-ui', 'sans-serif'],
        body: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['Fira Code', 'ui-monospace', 'monospace'],
      },
    },
  },
  plugins: [],
};
