#include <bitset>
#include <cstdint>
#include <iostream>
#include <string>

std::string toBinaryString(uint64_t value)
{
    std::string raw = std::bitset<64>(value).to_string();
    std::string result;
    // From little-endian to big-endian byte order.
    for (int i = 56; i >= 0; i -= 8)
    {
        result += raw.substr(i, 8);
        result += " ";
    }
    return result;
}

int main()
{
    const char *text = "FDE\nLEO\n";
    uint64_t block = *reinterpret_cast<const uint64_t *>(text);
    std::cout << "block                    = " << toBinaryString(block) << "\n";

    // 4.
    // Given 8 consecutive bytes in the variable block of type uint64_t,
    // which bitwise operation on block sets all bits in a byte to 0 if the byte
    // equals '\n' (or 1010 in binary representation or 0x0A in hexadecimal)?
    uint64_t newline_pattern = 0x0A'0A'0A'0A'0A'0A'0A'0A;
    std::cout << "newline_pattern          = " << toBinaryString(newline_pattern) << "\n";
    uint64_t all0iffNl = block ^ newline_pattern;
    std::cout << "all0iffNl                = " << toBinaryString(all0iffNl) << "\n";

    // 5.
    // For the variable all0iffNl, the highest bit in each byte is 0 (or in other words, each byte is <= 127).
    // Which operation on all0iffNl will set the highest bit to one in each byte iff the byte is not 0?
    // 0x7F == 127 == 01111111
    // 127 + 0 == 127 == 01111111
    // 127 + 1 == 128 == 10000000
    uint64_t low = 0x7F'7F'7F'7F'7F'7F'7F'7F;
    std::cout << "low                      = " << toBinaryString(low) << "\n";
    uint64_t highestBitSetIffByteNot0 = all0iffNl + low;
    std::cout << "highestBitSetIffByteNot0 = " << toBinaryString(highestBitSetIffByteNot0) << "\n";

    // 6.
    // Given the previous result, which operations need to be performed so that the highest bit is 1
    // and all others are 0 if the highest bit is not set and all 0 if the highest bit is set?
    uint64_t highestBitSetIffNl = ~highestBitSetIffByteNot0;
    std::cout << "highestBitSetIffNl       = " << toBinaryString(highestBitSetIffNl) << "\n";
    // 0x80 == 128 == 10000000
    uint64_t high = 0x80'80'80'80'80'80'80'80;
    std::cout << "high                     = " << toBinaryString(high) << "\n";
    uint64_t highestSetIf0 = highestBitSetIffNl & high;
    std::cout << "highestSetIf0            = " << toBinaryString(highestSetIf0) << "\n";

    // 8.
    // Previously, we assumed that all byte values are <= 127.
    // Adapt your solution so that it can also handle values > 127.
    // We still assume that the character we search for is <= 127.
    // The idea is that we just ignore all bytes in the block > 127.
    // !!! UNCOMMENT THE FOLLOWING CODE !!!
    // {
    //     uint64_t block; // the bytes in block have values in the range [0, 255]
    //     uint64_t newline_pattern = 0x0A'0A'0A'0A'0A'0A'0A'0A;
    //     uint64_t low = 0x7F'7F'7F'7F'7F'7F'7F'7F;
    //     uint64_t high = 0x80'80'80'80'80'80'80'80;

    //     uint64_t highestBitSetIffByteSmaller128 = ~block;
    //     // Remember the positions of all bytes smaller than 128.
    //     uint64_t onlyHighestBitSetIffByteSmaller128 = highestBitSetIffByteSmaller128 & high;
    //     // We need to set the highest bit to 0 so that addition does not overflow.
    //     uint64_t setHighestBitSetTo0 = block & low;
    //     uint64_t all0iffNl = setHighestBitSetTo0 ^ newline_pattern;

    //     uint64_t highestBitSetIffByteNot0 = all0iffNl + low;
    //     uint64_t highestBitSetIffNl = ~highestBitSetIffByteNot0;
    //     uint64_t highestSetIf0 = highestBitSetIffNl & high;
    //     uint64_t result = highestSetIf0 & onlyHighestBitSetIffByteSmaller128;
    // }
}