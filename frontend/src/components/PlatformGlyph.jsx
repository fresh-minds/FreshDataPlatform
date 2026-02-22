import React from 'react';

function PlatformGlyph() {
    return (
        <svg className="platform-glyph" viewBox="0 0 240 180" aria-hidden="true" focusable="false">
            <rect className="platform-glyph__frame" x="20" y="20" width="200" height="140" rx="24" />
            <circle className="platform-glyph__node" cx="70" cy="60" r="12" />
            <circle className="platform-glyph__node" cx="170" cy="60" r="12" />
            <circle className="platform-glyph__node" cx="70" cy="120" r="12" />
            <circle className="platform-glyph__node" cx="170" cy="120" r="12" />
            <circle className="platform-glyph__node platform-glyph__node--core" cx="120" cy="90" r="18" />
            <path className="platform-glyph__link" d="M80 64L103 81" />
            <path className="platform-glyph__link" d="M160 64L137 81" />
            <path className="platform-glyph__link" d="M80 116L103 99" />
            <path className="platform-glyph__link" d="M160 116L137 99" />
            <rect className="platform-glyph__chip" x="101" y="83" width="38" height="14" rx="7" />
        </svg>
    );
}

export default PlatformGlyph;
