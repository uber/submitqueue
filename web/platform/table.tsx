"use client";

import {
  StyledRoot,
  StyledTable,
  StyledTableBody,
  StyledTableHead,
  StyledTableHeadCell,
  StyledTableHeadRow,
} from "baseui/table-semantic";
import type { ReactNode } from "react";

export function Table({ label, headers, children, height }: {
  label: string;
  headers: string[];
  children: ReactNode;
  height?: string;
}) {
  return <StyledRoot tabIndex={0} role="region" aria-label={label} $style={height ? { height, overscrollBehavior: "contain" } : {}}>
    <StyledTable aria-label={label}>
      <StyledTableHead><StyledTableHeadRow>
        {headers.map(header => <StyledTableHeadCell key={header} scope="col">{header}</StyledTableHeadCell>)}
      </StyledTableHeadRow></StyledTableHead>
      <StyledTableBody>{children}</StyledTableBody>
    </StyledTable>
  </StyledRoot>;
}
